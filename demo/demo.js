/* logstream interactive demo
 * vanilla JS, no dependencies. synthesises events when API is unreachable. */
(() => {
  'use strict';

  // -------- config --------
  const SERVICES = [
    'api-gateway', 'user-service', 'order-service', 'payment-service',
    'inventory-service', 'notification-service', 'auth-service', 'search-service'
  ];
  const LEVELS = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'];
  const LEVEL_WEIGHTS = [10, 70, 14, 5, 1]; // %
  const LEVEL_COLOR = { DEBUG: '#6b7280', INFO: '#2563eb', WARN: '#d97706', ERROR: '#dc2626', FATAL: '#7c2d12' };
  const MSG_BANK = {
    INFO: ['request handled', 'user authenticated', 'order placed', 'cache hit', 'health check ok', 'job scheduled', 'connection established', 'query completed'],
    DEBUG: ['ctx propagated', 'span emitted', 'span closed', 'pool acquired', 'pool released', 'token refreshed'],
    WARN: ['slow query 312ms', 'retry attempt 2/5', 'rate limit approaching', 'cache miss', 'connection pool 80% full', 'deprecated endpoint'],
    ERROR: ['db connection refused', 'upstream timeout', 'auth failed for user', 'invalid payload', 'circuit breaker open', 'mongo write conflict'],
    FATAL: ['out of memory', 'replica set primary lost', 'kafka broker unreachable']
  };
  const ROLL_WINDOW_MS = 60_000;
  const TICK_MS = 1000;
  const EPS_BASELINE = [38, 52]; // synthetic baseline events per tick

  // -------- state --------
  const state = {
    buffer: [],          // recent events (rolling 60s)
    perSecond: [],       // {t, count} for chart
    levelCounts: { DEBUG: 0, INFO: 0, WARN: 0, ERROR: 0, FATAL: 0 },
    svcCounts: new Map(),
    totalIngested: 0,
    accepted: 0,
    dropped: 0,
    queueDepth: 0,
    paused: false,
    latencySamples: [], // last 100 simulated write-latency samples
    filters: { service: 'all', level: 'all', q: '', windowSec: 60 },
    alerts: []
  };

  // -------- DOM refs --------
  const $ = (id) => document.getElementById(id);

  // ============================================================
  // KPI counters — animated count-up on load
  // ============================================================
  function animateKpis() {
    document.querySelectorAll('.kpi-num').forEach((el) => {
      const target = parseFloat(el.dataset.target) || 0;
      const suffix = el.dataset.suffix || '';
      const dur = 1400;
      const start = performance.now();
      const tick = (now) => {
        const p = Math.min(1, (now - start) / dur);
        const eased = 1 - Math.pow(1 - p, 3);
        const v = target * eased;
        el.textContent = formatKpi(v, target) + suffix;
        if (p < 1) requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
    });
  }
  function formatKpi(v, target) {
    if (target >= 1000) return Math.round(v).toLocaleString();
    if (target % 1 === 0) return Math.round(v).toString();
    return v.toFixed(1);
  }

  // ============================================================
  // synthetic event generator
  // ============================================================
  function pickLevel() {
    const r = Math.random() * 100;
    let acc = 0;
    for (let i = 0; i < LEVELS.length; i++) {
      acc += LEVEL_WEIGHTS[i];
      if (r < acc) return LEVELS[i];
    }
    return 'INFO';
  }
  function pickSvc() { return SERVICES[Math.floor(Math.random() * SERVICES.length)]; }
  function pickMsg(level) {
    const bank = MSG_BANK[level] || MSG_BANK.INFO;
    return bank[Math.floor(Math.random() * bank.length)];
  }
  function makeEvent(forcedLevel) {
    const lvl = forcedLevel || pickLevel();
    return {
      ts: Date.now(),
      level: lvl,
      service: pickSvc(),
      message: pickMsg(lvl),
      traceId: Math.random().toString(36).slice(2, 10),
      latencyMs: simulateLatency()
    };
  }
  function simulateLatency() {
    // log-normalish: most around 35–55ms, long tail
    const base = 28 + Math.random() * 22;
    const tail = Math.random() < 0.05 ? Math.random() * 180 : 0;
    return Math.round(base + tail);
  }

  function ingest(ev) {
    state.buffer.push(ev);
    state.totalIngested += 1;
    state.levelCounts[ev.level] = (state.levelCounts[ev.level] || 0) + 1;
    state.svcCounts.set(ev.service, (state.svcCounts.get(ev.service) || 0) + 1);
    state.latencySamples.push(ev.latencyMs);
    if (state.latencySamples.length > 200) state.latencySamples.shift();
    // queue depth simulation: occasional spike then drain
    state.queueDepth = Math.max(0, state.queueDepth + (Math.random() < 0.5 ? 1 : -1));
    // 1% drop simulation under bursts
    if (Math.random() < 0.005) { state.dropped += 1; }
    else { state.accepted += 1; }
    // explorer feed cap
    if (state.buffer.length > 2000) state.buffer.shift();
    // alerts
    if (ev.level === 'ERROR' || ev.level === 'FATAL') pushAlert(ev);
  }

  function pushAlert(ev) {
    const alert = {
      ts: ev.ts,
      level: ev.level,
      service: ev.service,
      message: ev.message,
    };
    state.alerts.unshift(alert);
    if (state.alerts.length > 12) state.alerts.length = 12;
    renderAlerts();
  }

  // ============================================================
  // periodic tick — generate events, refresh charts
  // ============================================================
  let lastTick = Date.now();
  function tick() {
    if (state.paused) { lastTick = Date.now(); return; }
    const n = randInt(EPS_BASELINE[0], EPS_BASELINE[1]);
    for (let i = 0; i < n; i++) ingest(makeEvent());

    // roll window — prune events older than ROLL_WINDOW_MS
    const cutoff = Date.now() - ROLL_WINDOW_MS;
    while (state.buffer.length && state.buffer[0].ts < cutoff) state.buffer.shift();

    state.perSecond.push({ t: Date.now(), count: n });
    if (state.perSecond.length > 60) state.perSecond.shift();

    refreshAll();
    lastTick = Date.now();
  }
  function randInt(a, b) { return a + Math.floor(Math.random() * (b - a + 1)); }

  function refreshAll() {
    renderEps();
    renderThroughputChart();
    renderGauge();
    renderSeverityBars();
    renderServiceGrid();
    renderLatencyChart();
    renderLatencyPills();
    renderExplorer();
    renderQueueStats();
  }

  // ============================================================
  // renderers
  // ============================================================
  function renderEps() {
    const last = state.perSecond[state.perSecond.length - 1];
    $('stat-eps').innerHTML = `${(last ? last.count : 0).toLocaleString()} <small>eps</small>`;
    $('total-ingested').textContent = `${state.totalIngested.toLocaleString()} events total`;
  }
  function renderQueueStats() {
    $('stat-depth').textContent = state.queueDepth.toLocaleString();
    $('stat-accepted').textContent = state.accepted.toLocaleString();
    $('stat-dropped').textContent = state.dropped.toLocaleString();
  }

  function renderThroughputChart() {
    const c = $('chart-throughput'); if (!c) return;
    fitCanvas(c);
    const ctx = c.getContext('2d');
    const w = c.clientWidth, h = c.clientHeight;
    ctx.clearRect(0, 0, w, h);
    const data = state.perSecond.slice(-60);
    if (data.length < 2) return;
    const max = Math.max(60, ...data.map(d => d.count)) * 1.15;
    const stepX = w / (data.length - 1);

    // grid
    ctx.strokeStyle = 'rgba(20,20,20,0.06)';
    ctx.lineWidth = 1;
    for (let i = 0; i < 4; i++) {
      const y = (h / 4) * i + 0.5;
      ctx.beginPath(); ctx.moveTo(0, y); ctx.lineTo(w, y); ctx.stroke();
    }
    // area
    const accent = getCssVar('--accent') || '#ff4d2e';
    ctx.fillStyle = hexA(accent, 0.14);
    ctx.beginPath();
    ctx.moveTo(0, h);
    data.forEach((d, i) => {
      const x = i * stepX;
      const y = h - (d.count / max) * h;
      ctx.lineTo(x, y);
    });
    ctx.lineTo(w, h); ctx.closePath(); ctx.fill();
    // line
    ctx.strokeStyle = accent; ctx.lineWidth = 2;
    ctx.beginPath();
    data.forEach((d, i) => {
      const x = i * stepX;
      const y = h - (d.count / max) * h;
      if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
    });
    ctx.stroke();
    // last point dot
    const lastPt = data[data.length - 1];
    const lx = (data.length - 1) * stepX;
    const ly = h - (lastPt.count / max) * h;
    ctx.fillStyle = accent;
    ctx.beginPath(); ctx.arc(lx, ly, 3.5, 0, Math.PI * 2); ctx.fill();
  }

  function renderGauge() {
    // gauge: pct = (eps / capacity) capped
    const last = state.perSecond[state.perSecond.length - 1];
    const eps = last ? last.count : 0;
    const cap = 120; // visual capacity reference
    const pct = Math.min(1, eps / cap);
    const arc = $('gauge-arc');
    if (arc) {
      const len = 251.3; // approx half-circle length (r=80 → πr ≈ 251.3)
      arc.style.strokeDasharray = `${len}`;
      arc.style.strokeDashoffset = `${len * (1 - pct)}`;
    }
    const pctEl = $('gauge-pct');
    if (pctEl) pctEl.textContent = `${Math.round(pct * 100)}%`;
  }

  function renderSeverityBars() {
    const counts = state.levelCounts;
    const max = Math.max(1, ...Object.values(counts));
    LEVELS.forEach((lvl) => {
      const bar = $(`bar-${lvl}`);
      const val = $(`val-${lvl}`);
      if (bar) bar.style.width = `${(counts[lvl] / max) * 100}%`;
      if (val) val.textContent = (counts[lvl] || 0).toLocaleString();
    });
  }

  function renderServiceGrid() {
    const grid = $('service-grid');
    if (!grid) return;
    const entries = SERVICES.map((s) => [s, state.svcCounts.get(s) || 0]);
    const counts = entries.map(e => e[1]);
    const max = Math.max(1, ...counts);
    grid.innerHTML = entries.map(([svc, c]) => {
      const ratio = c / max;
      const heat = ratio > 0.75 ? 4 : ratio > 0.5 ? 3 : ratio > 0.25 ? 2 : ratio > 0.05 ? 1 : 0;
      return `<div class="svc-cell" data-heat="${heat}" title="${svc}: ${c} events">
        <span class="svc-name">${svc}</span>
        <span class="svc-count">${c.toLocaleString()}</span>
      </div>`;
    }).join('');
  }

  function renderLatencyChart() {
    const c = $('chart-latency'); if (!c) return;
    fitCanvas(c);
    const ctx = c.getContext('2d');
    const w = c.clientWidth, h = c.clientHeight;
    ctx.clearRect(0, 0, w, h);
    if (state.latencySamples.length < 5) return;
    // histogram: buckets 0–250 in 10ms steps
    const buckets = new Array(25).fill(0);
    state.latencySamples.forEach((v) => {
      const idx = Math.min(buckets.length - 1, Math.floor(v / 10));
      buckets[idx] += 1;
    });
    const max = Math.max(1, ...buckets);
    const bw = w / buckets.length;
    const accent = getCssVar('--accent') || '#ff4d2e';
    buckets.forEach((b, i) => {
      const bh = (b / max) * (h - 18);
      ctx.fillStyle = hexA(accent, 0.55);
      ctx.fillRect(i * bw + 1, h - bh - 14, bw - 2, bh);
    });
    // axis labels
    ctx.fillStyle = 'rgba(20,20,20,0.5)';
    ctx.font = '10px JetBrains Mono, monospace';
    ['0', '50ms', '100ms', '150ms', '200ms', '250ms'].forEach((lbl, i) => {
      ctx.fillText(lbl, (i / 5) * (w - 28), h - 2);
    });
  }

  function renderLatencyPills() {
    const s = state.latencySamples.slice().sort((a, b) => a - b);
    if (!s.length) return;
    const p = (q) => s[Math.min(s.length - 1, Math.floor(s.length * q))];
    $('lat-p50') && ($('lat-p50').textContent = `${p(0.50)}ms`);
    $('lat-p95') && ($('lat-p95').textContent = `${p(0.95)}ms`);
    $('lat-p99') && ($('lat-p99').textContent = `${p(0.99)}ms`);
  }

  function renderAlerts() {
    const ul = $('alert-feed');
    if (!ul) return;
    ul.innerHTML = state.alerts.map((a) => {
      const t = new Date(a.ts);
      const hh = String(t.getHours()).padStart(2, '0');
      const mm = String(t.getMinutes()).padStart(2, '0');
      const ss = String(t.getSeconds()).padStart(2, '0');
      return `<li class="alert a-${a.level}">
        <span class="alert-time mono">${hh}:${mm}:${ss}</span>
        <span class="alert-level">${a.level}</span>
        <span class="alert-svc mono">${a.service}</span>
        <span class="alert-msg">${escapeHtml(a.message)}</span>
      </li>`;
    }).join('');
  }

  // ============================================================
  // explorer
  // ============================================================
  function populateServiceSelect() {
    const sel = $('f-service');
    if (!sel) return;
    const opts = ['<option value="all">all services</option>']
      .concat(SERVICES.map(s => `<option value="${s}">${s}</option>`));
    sel.innerHTML = opts.join('');
  }

  function renderExplorer() {
    const box = $('log-rows');
    if (!box) return;
    const cutoff = Date.now() - state.filters.windowSec * 1000;
    const q = state.filters.q.toLowerCase();
    const rows = state.buffer.filter(e =>
      e.ts >= cutoff
      && (state.filters.service === 'all' || e.service === state.filters.service)
      && (state.filters.level === 'all' || e.level === state.filters.level)
      && (q === '' || e.message.toLowerCase().includes(q) || e.service.toLowerCase().includes(q))
    ).slice(-200).reverse();

    $('match-count') && ($('match-count').textContent = rows.length.toLocaleString());

    box.innerHTML = rows.map((e) => {
      const t = new Date(e.ts);
      const hh = String(t.getHours()).padStart(2, '0');
      const mm = String(t.getMinutes()).padStart(2, '0');
      const ss = String(t.getSeconds()).padStart(2, '0');
      const ms = String(t.getMilliseconds()).padStart(3, '0');
      return `<div class="log-row">
        <span class="log-ts mono">${hh}:${mm}:${ss}.${ms}</span>
        <span class="log-lvl" style="color:${LEVEL_COLOR[e.level]}">${e.level}</span>
        <span class="log-svc mono">${e.service}</span>
        <span class="log-msg">${escapeHtml(e.message)}</span>
        <span class="log-trace mono">${e.traceId}</span>
      </div>`;
    }).join('');
  }

  function bindExplorer() {
    const sync = () => {
      state.filters.service = $('f-service').value;
      state.filters.level = $('f-level').value;
      state.filters.q = $('f-q').value || '';
      state.filters.windowSec = parseInt($('f-window').value || '60', 10);
      renderExplorer();
    };
    ['f-service', 'f-level', 'f-window'].forEach(id => $(id) && $(id).addEventListener('change', sync));
    $('f-q') && $('f-q').addEventListener('input', sync);
    $('f-clear') && $('f-clear').addEventListener('click', () => {
      $('f-service').value = 'all';
      $('f-level').value = 'all';
      $('f-q').value = '';
      $('f-window').value = '60';
      sync();
    });
  }

  // ============================================================
  // control buttons
  // ============================================================
  function bindControls() {
    $('btn-burst') && $('btn-burst').addEventListener('click', () => {
      for (let i = 0; i < 200; i++) ingest(makeEvent());
      flashButton('btn-burst');
      refreshAll();
    });
    $('btn-error') && $('btn-error').addEventListener('click', () => {
      for (let i = 0; i < 10; i++) ingest(makeEvent('ERROR'));
      ingest(makeEvent('FATAL'));
      flashButton('btn-error');
      refreshAll();
    });
    $('btn-pause') && $('btn-pause').addEventListener('click', () => {
      state.paused = !state.paused;
      const lbl = $('pause-label');
      if (lbl) lbl.textContent = state.paused ? '▶ resume' : '⏸ pause';
    });
  }
  function flashButton(id) {
    const b = $(id); if (!b) return;
    b.classList.add('flash');
    setTimeout(() => b.classList.remove('flash'), 400);
  }

  // ============================================================
  // architecture hover tips
  // ============================================================
  function bindArchTips() {
    const tip = $('arch-tip');
    if (!tip) return;
    document.querySelectorAll('g.arch-box').forEach((g) => {
      g.addEventListener('mouseenter', (e) => {
        tip.textContent = g.dataset.tip || '';
        tip.classList.add('show');
      });
      g.addEventListener('mousemove', (e) => {
        const host = tip.parentElement;
        const r = host.getBoundingClientRect();
        tip.style.left = (e.clientX - r.left + 14) + 'px';
        tip.style.top = (e.clientY - r.top + 14) + 'px';
      });
      g.addEventListener('mouseleave', () => tip.classList.remove('show'));
    });
  }

  // ============================================================
  // code tabs
  // ============================================================
  function bindTabs() {
    document.querySelectorAll('.tab').forEach((t) => {
      t.addEventListener('click', () => {
        const target = t.dataset.tab;
        document.querySelectorAll('.tab').forEach(x => x.classList.toggle('active', x === t));
        document.querySelectorAll('[id^="code-"]').forEach(c => c.classList.toggle('hidden', c.id !== `code-${target}`));
      });
    });
  }

  // ============================================================
  // connection probe — tries /api/v1/pipeline; falls back to demo mode
  // ============================================================
  async function probeConnection() {
    const dot = $('conn-dot'); const text = $('conn-text');
    try {
      const ctrl = new AbortController();
      const to = setTimeout(() => ctrl.abort(), 1200);
      const r = await fetch('/api/v1/pipeline', { signal: ctrl.signal });
      clearTimeout(to);
      if (r.ok) {
        dot && (dot.style.background = '#1f9d55');
        text && (text.textContent = 'connected to live api');
        return;
      }
      throw new Error('bad response');
    } catch (_) {
      dot && (dot.style.background = '#9aa0a6');
      text && (text.textContent = 'demo mode · synthetic stream');
    }
  }

  // ============================================================
  // smooth scroll for anchor nav
  // ============================================================
  function bindNav() {
    document.querySelectorAll('a[href^="#"]').forEach(a => {
      a.addEventListener('click', (e) => {
        const id = a.getAttribute('href').slice(1);
        const el = document.getElementById(id);
        if (!el) return;
        e.preventDefault();
        el.scrollIntoView({ behavior: 'smooth', block: 'start' });
      });
    });
  }

  // ============================================================
  // helpers
  // ============================================================
  function fitCanvas(c) {
    const dpr = window.devicePixelRatio || 1;
    const rect = c.getBoundingClientRect();
    if (c.width !== rect.width * dpr || c.height !== rect.height * dpr) {
      c.width = rect.width * dpr;
      c.height = rect.height * dpr;
      const ctx = c.getContext('2d');
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    }
  }
  function getCssVar(name) {
    return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  }
  function hexA(hex, a) {
    const h = hex.replace('#', '');
    const r = parseInt(h.slice(0, 2), 16);
    const g = parseInt(h.slice(2, 4), 16);
    const b = parseInt(h.slice(4, 6), 16);
    return `rgba(${r},${g},${b},${a})`;
  }
  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, m => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
  }

  // ============================================================
  // boot
  // ============================================================
  function boot() {
    animateKpis();
    populateServiceSelect();
    bindExplorer();
    bindControls();
    bindArchTips();
    bindTabs();
    bindNav();
    probeConnection();
    // prime with some synthetic data so first paint isn't empty
    for (let i = 0; i < 250; i++) {
      const e = makeEvent();
      e.ts = Date.now() - Math.random() * ROLL_WINDOW_MS;
      ingest(e);
    }
    refreshAll();
    setInterval(tick, TICK_MS);
    window.addEventListener('resize', () => {
      renderThroughputChart();
      renderLatencyChart();
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
