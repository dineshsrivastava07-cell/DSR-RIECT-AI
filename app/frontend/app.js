/**
 * DSR|RIECT — Frontend v2
 * Fixes: model auto-detect · history pane · response header · tab-aware rendering · account auth
 */
'use strict';

const API = '/api';
const WS_URL = `ws://${location.host}/ws/chat`;

const state = {
  ws: null,
  sessionId: null,
  isStreaming: false,
  currentMsgEl: null,
  charts: new Map(),
  allAlerts: [],
  activeAlertFilter: 'all',
  historyOpen: false,
  activeSessionId: null,
  rawNarrative: '',   // accumulates streaming tokens for markdown render
};

// ─── Markdown renderer (safe HTML) ────────────────────────────────────────────
function renderMarkdown(text) {
  if (typeof marked === 'undefined') return text.replace(/\n/g, '<br>');
  try {
    let html = marked.parse(text, { breaks: true, gfm: true });
    // Wrap every markdown table in a scrollable container (horizontal + vertical scroll)
    html = html.replace(/<table>/g, '<div class="narrative-table-wrap"><table>');
    html = html.replace(/<\/table>/g, '</table></div>');
    return html;
  } catch (_) {
    return text.replace(/\n/g, '<br>');
  }
}

// ─── Number formatting ────────────────────────────────────────────────────────
const MONETARY_COLS = new Set(['net_sales_amount','total_gross','total_discount',
  'total_promo','total_mrp','chain_net_sales','net','gross','disc','promo',
  'return_amt','gross_sales','non_promo_disc','revenue','atv','store_net']);
const PCT_COLS = new Set(['sell_thru_pct','disc_rate_pct','return_rate_pct',
  'bill_integrity','sell_thru_pct_display']);
const DECIMAL_COLS = new Set(['spsf','doi','upt','avg_daily_sales','bill_integrity']);

function formatCellValue(val, colName) {
  if (val === null || val === undefined || val === '') return '—';
  const col = colName.toLowerCase();
  if (typeof val === 'number') {
    if (MONETARY_COLS.has(col)) {
      return '₹' + val.toLocaleString('en-IN', { maximumFractionDigits: 0 });
    }
    if (PCT_COLS.has(col)) {
      return val.toFixed(1) + '%';
    }
    if (DECIMAL_COLS.has(col)) {
      return val.toFixed(2);
    }
    // Integer-like cols
    if (Number.isInteger(val) || col.includes('count') || col.includes('qty') || col.includes('bills')) {
      return val.toLocaleString('en-IN');
    }
    return val.toLocaleString('en-IN', { maximumFractionDigits: 2 });
  }
  return String(val);
}

// ─── Init ─────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
  await loadStatus();          // detect Ollama models, populate LLM selector
  await initSession();
  connectWebSocket();
  loadSettings();
  loadKPIDashboard();
  loadAlerts();
  loadHistory();
  bindInputEvents();
  bindTabDelegation();
  bindExceptionFilters();
  bindHistoryToggle();
});

// ─── Status / Model Detection ─────────────────────────────────────────────────
async function loadStatus() {
  try {
    const res = await fetch(`${API}/status`);
    const data = await res.json();

    const selector = document.getElementById('llm-selector');
    selector.innerHTML = '';

    // Ollama models
    const ollama = data.ollama || {};
    if (ollama.available && ollama.models.length > 0) {
      const grp = document.createElement('optgroup');
      grp.label = '⚡ Local (Ollama)';
      ollama.models.forEach(m => {
        const opt = document.createElement('option');
        opt.value = m;
        opt.textContent = m;
        if (m === ollama.best_model) opt.selected = true;
        grp.appendChild(opt);
      });
      selector.appendChild(grp);
    }

    // Cloud providers
    const providers = data.llm_providers || {};
    const cloudGrp = document.createElement('optgroup');
    cloudGrp.label = '☁ Cloud';

    // Qwen — first, always show if connected (3 sub-models)
    if (providers.qwen) {
      [
        ['qwen3.5-plus',  '✦ Qwen3.5-Plus ✓'],
        ['qwen3.5-flash', '✦ Qwen3.5 Flash ✓'],
        ['qwen3-max',     '✦ Qwen3-Max ✓'],
      ].forEach(([k, label]) => {
        const opt = document.createElement('option');
        opt.value = k; opt.textContent = label;
        cloudGrp.appendChild(opt);
      });
    }

    // Other providers
    [['claude', '✦ Claude'], ['gemini', '◈ Gemini'], ['openai', '◉ ChatGPT']].forEach(([k, label]) => {
      if (providers[k]) {
        const opt = document.createElement('option');
        opt.value = k;
        opt.textContent = label + ' ✓';
        cloudGrp.appendChild(opt);
      }
    });
    if (cloudGrp.children.length > 0) selector.appendChild(cloudGrp);

    // Fallback
    if (selector.options.length === 0) {
      selector.innerHTML = '<option value="auto">No LLM — configure in ⚙ Settings</option>';
    }

    // Select active model — Qwen always wins when connected
    const QWEN_IDS = new Set(['qwen3.5-plus', 'qwen3.5-flash', 'qwen3-max', 'qwen']);
    const savedModel = localStorage.getItem('riect_selected_llm');
    if (providers.qwen) {
      // If saved model is already a Qwen model, respect it; otherwise force qwen3.5-plus
      const target = (savedModel && QWEN_IDS.has(savedModel)) ? savedModel : 'qwen3.5-plus';
      const opt = [...selector.options].find(o => o.value === target);
      if (opt) { selector.value = target; localStorage.setItem('riect_selected_llm', target); }
    } else if (savedModel) {
      const matchOpt = [...selector.options].find(o => o.value === savedModel);
      if (matchOpt) selector.value = savedModel;
    }
    updateModelBadge(selector.value);

    // Immediately apply selected model and update badge whenever user changes selector
    selector.addEventListener('change', () => {
      const selected = selector.value;
      localStorage.setItem('riect_selected_llm', selected);
      updateModelBadge(selected);
    });

  } catch (e) {
    console.warn('Status load failed:', e);
  }
}

function updateModelBadge(modelVal) {
  const badge = document.getElementById('model-badge');
  if (!badge) return;
  const labels = {
    'qwen3.5-plus':  'Qwen3.5-Plus',
    'qwen3.5-flash': 'Qwen3.5 Flash',
    'qwen3-max':     'Qwen3-Max',
    'qwen':          'Qwen',
    'claude':        'Claude',
    'gemini':        'Gemini',
    'openai':        'ChatGPT',
    'auto':          'Auto',
  };
  badge.textContent = labels[modelVal] || modelVal || 'Auto';
}

// ─── Session ──────────────────────────────────────────────────────────────────
async function initSession() {
  try {
    const res = await fetch(`${API}/sessions`, {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({title:'New Chat', role:'HQ'})});
    const data = await res.json();
    state.sessionId = data.session_id;
    state.activeSessionId = data.session_id;
  } catch (e) {
    state.sessionId = 'sess_' + Date.now();
    state.activeSessionId = state.sessionId;
  }
}

async function loadSession(sessionId) {
  if (state.isStreaming) return;
  state.sessionId = sessionId;
  state.activeSessionId = sessionId;

  // Clear chat
  const container = document.getElementById('chat-container');
  container.innerHTML = '';

  // Load messages
  try {
    const res = await fetch(`${API}/sessions/${sessionId}/messages`);
    const msgs = await res.json();

    if (msgs.length === 0) {
      showWelcome();
      return;
    }

    msgs.forEach(m => {
      if (m.role === 'user') {
        appendUserMessage(m.content);
      } else {
        const el = appendAssistantMessage();
        const narrative = el.querySelector('.response-narrative');
        if (narrative) narrative.textContent = m.content;
        el.classList.remove('msg-streaming');
      }
    });
    scrollChat();
  } catch (e) {
    showWelcome();
  }

  // Mark active in history
  document.querySelectorAll('.hist-item').forEach(i => i.classList.remove('active'));
  const item = document.querySelector(`.hist-item[data-id="${sessionId}"]`);
  if (item) item.classList.add('active');
}

async function newChat() {
  if (state.isStreaming) return;
  try {
    const res = await fetch(`${API}/sessions`, {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({title:'New Chat'})});
    const data = await res.json();
    state.sessionId = data.session_id;
    state.activeSessionId = data.session_id;
    const container = document.getElementById('chat-container');
    container.innerHTML = '';
    showWelcome();
    loadHistory();
  } catch (e) {}
}

// ─── History Pane ─────────────────────────────────────────────────────────────
function bindHistoryToggle() {
  document.getElementById('history-toggle').addEventListener('click', toggleHistory);
  document.getElementById('new-chat-btn').addEventListener('click', newChat);
  document.getElementById('new-chat-hist').addEventListener('click', newChat);
}

function toggleHistory() {
  const pane = document.getElementById('history-pane');
  state.historyOpen = !state.historyOpen;
  pane.classList.toggle('open', state.historyOpen);
}

async function loadHistory() {
  try {
    const res = await fetch(`${API}/sessions?limit=30`);
    const sessions = await res.json();
    renderHistory(sessions);
  } catch (e) {}
}

function renderHistory(sessions) {
  const list = document.getElementById('history-list');
  list.innerHTML = '';

  if (!sessions.length) {
    list.innerHTML = '<div class="hist-empty">No sessions yet</div>';
    return;
  }

  sessions.forEach(s => {
    const item = document.createElement('div');
    item.className = 'hist-item' + (s.session_id === state.activeSessionId ? ' active' : '');
    item.dataset.id = s.session_id;
    const date = new Date(s.created_at).toLocaleDateString();
    item.innerHTML = `
      <div class="hist-item-title" title="${s.title || 'Chat'}">${s.title || 'Chat'}</div>
      <div class="hist-item-meta">${date} · ${s.msg_count || 0} messages</div>
      <div class="hist-item-actions">
        <button class="hist-del-btn" title="Delete" onclick="deleteSession('${s.session_id}', event)">🗑</button>
      </div>
    `;
    item.addEventListener('click', () => loadSession(s.session_id));
    list.appendChild(item);
  });
}

async function deleteSession(sessionId, event) {
  event.stopPropagation();
  try {
    await fetch(`${API}/sessions/${sessionId}`, {method: 'DELETE'});
    if (sessionId === state.sessionId) await newChat();
    loadHistory();
  } catch (e) {}
}

// ─── WebSocket ────────────────────────────────────────────────────────────────
function connectWebSocket() {
  setStatus('connecting', 'Connecting...');
  try {
    state.ws = new WebSocket(WS_URL);
    state.ws.onopen = () => { setStatus('connected', 'Ready'); };
    state.ws.onmessage = (ev) => {
      try { handleWsMessage(JSON.parse(ev.data)); } catch (e) {}
    };
    state.ws.onclose = () => {
      setStatus('error', 'Reconnecting...');
      setTimeout(connectWebSocket, 3000);
    };
    state.ws.onerror = () => setStatus('error', 'Connection error');
  } catch (e) {
    setTimeout(connectWebSocket, 5000);
  }
}

function handleWsMessage(msg) {
  switch (msg.type) {
    case 'model':
      updateModelBadge(msg.model || document.getElementById('llm-selector').value);
      if (state.currentMsgEl) {
        const lbl = state.currentMsgEl.querySelector('.msg-model-label');
        if (lbl) lbl.textContent = msg.model;
      }
      break;

    case 'intent':
      if (state.currentMsgEl && msg.intent) {
        const lbl = state.currentMsgEl.querySelector('.msg-intent-label');
        if (lbl) {
          lbl.textContent = msg.intent.replace(/_/g, ' ');
          lbl.style.display = '';
        }
      }
      updateStatus(`Analysing: ${(msg.intent || '').replace(/_/g,' ')}...`);
      break;

    case 'sql_generated':
      addPipelinePip('SQL generated', 'sql');
      // Pre-fill SQL tab immediately
      if (state.currentMsgEl) {
        const sqlEl = state.currentMsgEl.querySelector('.sql-code');
        if (sqlEl) sqlEl.textContent = msg.sql;
        const tabs = state.currentMsgEl.querySelector('.response-tabs');
        if (tabs) tabs.style.display = '';
      }
      break;

    case 'data_ready':
      addPipelinePip(`${msg.rows} rows retrieved`, 'data');
      updateStatus(`${msg.rows} rows — generating insights...`);
      break;

    case 'sql_error':
      addPipelinePip(`SQL error: ${msg.message}`, 'error');
      break;

    case 'status':
      addPipelinePip(msg.message, 'status');
      break;

    case 'token':
      if (state.currentMsgEl) {
        state.rawNarrative += msg.content;
        const narrative = state.currentMsgEl.querySelector('.response-narrative');
        // Show raw text during streaming for performance; converted to HTML on 'done'
        if (narrative) narrative.textContent = state.rawNarrative;
        scrollChat();
      }
      break;

    case 'done':
      finaliseMsgElement(msg);
      loadHistory(); // refresh history after new message
      break;

    case 'error':
      if (state.currentMsgEl) {
        const narrative = state.currentMsgEl.querySelector('.response-narrative');
        if (narrative) {
          narrative.textContent = msg.message || 'An error occurred.';
          narrative.style.color = 'var(--p1)';
        }
      }
      endStreaming();
      break;
  }
}

// ─── Send ─────────────────────────────────────────────────────────────────────
function sendMessage() {
  const input = document.getElementById('chat-input');
  const query = input.value.trim();
  if (!query || state.isStreaming) return;
  input.value = '';
  submitQuery(query);
}

function sendSuggestion(btn) {
  document.getElementById('chat-input').value = btn.textContent.trim();
  sendMessage();
}

function submitQuery(query) {
  if (!state.ws || state.ws.readyState !== WebSocket.OPEN) {
    connectWebSocket();
    setTimeout(() => submitQuery(query), 1500);
    return;
  }

  // Remove welcome
  const w = document.getElementById('chat-welcome');
  if (w) w.remove();

  appendUserMessage(query);
  state.currentMsgEl = appendAssistantMessage();
  state.isStreaming = true;
  document.getElementById('send-btn').disabled = true;
  document.getElementById('chat-input').disabled = true;

  const llm = document.getElementById('llm-selector').value;

  state.ws.send(JSON.stringify({
    session_id: state.sessionId || '',
    message: query,
    role: document.getElementById('role-select').value,
    llm: llm === 'auto' ? null : llm,
  }));
  updateStatus('Processing...');
}

// ─── DOM Builders ─────────────────────────────────────────────────────────────
function appendUserMessage(text) {
  const tpl = document.getElementById('tpl-user-msg').content.cloneNode(true);
  tpl.querySelector('.msg-bubble').textContent = text;
  document.getElementById('chat-container').appendChild(tpl);
  scrollChat();
}

function appendAssistantMessage() {
  state.rawNarrative = '';  // reset for new message
  const tpl = document.getElementById('tpl-assistant-msg').content.cloneNode(true);
  const el = tpl.querySelector('.assistant-msg');
  el.classList.add('msg-streaming');
  document.getElementById('chat-container').appendChild(tpl);
  const all = document.querySelectorAll('#chat-container .assistant-msg');
  scrollChat();
  return all[all.length - 1];
}

function finaliseMsgElement(msg) {
  const blocks = msg.blocks || {};
  const el = state.currentMsgEl;
  if (!el) { endStreaming(); return; }

  el.classList.remove('msg-streaming');

  // Render accumulated raw narrative as markdown HTML
  const narrativeEl = el.querySelector('.response-narrative');
  if (narrativeEl && state.rawNarrative) {
    narrativeEl.innerHTML = renderMarkdown(state.rawNarrative);
  }

  const hasData = blocks.table && blocks.table.headers && blocks.table.headers.length > 0;
  const hasSql  = blocks.sql_artefact && blocks.sql_artefact.sql;
  const hasChart = blocks.chart && blocks.chart.labels && blocks.chart.labels.length > 0;

  if (hasData || hasSql) {
    const tabs = el.querySelector('.response-tabs');
    if (tabs) tabs.style.display = '';

    if (hasData)  renderTable(el, blocks.table);
    if (hasChart) renderChart(el, blocks.chart);
    if (hasSql)   renderSQL(el, blocks.sql_artefact);

    // Show tab that has the most meaningful data
    if (hasData)  activateTab(el, 'table');
    else if (hasChart) activateTab(el, 'chart');
    else if (hasSql)   activateTab(el, 'sql');
  }

  renderAlertStrip(el, blocks.alerts || []);

  if (blocks.alerts && blocks.alerts.length > 0) {
    blocks.alerts.forEach(a => { if (!state.allAlerts.find(x => x.alert_id === a.alert_id)) state.allAlerts.push(a); });
    renderExceptionInbox();
    updateAlertBadge();
  }

  if (msg.kpi_summary) updateKPIFromSummary(msg.kpi_summary);

  endStreaming();
  scrollChat();
}

function activateTab(el, tabName) {
  el.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tabName));
  el.querySelectorAll('.tab-panel').forEach(p => p.classList.toggle('active', p.dataset.panel === tabName));
}

// ─── Table ────────────────────────────────────────────────────────────────────
function renderTable(el, tableData) {
  if (!tableData || !tableData.headers.length) return;
  const thead = el.querySelector('.data-table thead');
  const tbody = el.querySelector('.data-table tbody');
  const badge = el.querySelector('.row-count-badge');
  const filterInput = el.querySelector('.table-filter-input');
  const colSelect = el.querySelector('.col-filter-select');

  const headers = tableData.headers;
  const rows = tableData.rows || [];

  // Header row
  const tr = document.createElement('tr');
  headers.forEach((h, i) => {
    const th = document.createElement('th');
    th.textContent = h.replace(/_/g, ' ');
    th.addEventListener('click', () => sortTable(tbody, rows, headers, i, th));
    tr.appendChild(th);
  });
  thead.appendChild(tr);

  // Column filter dropdown
  colSelect.innerHTML = '<option value="">All columns</option>';
  headers.forEach(h => {
    const opt = document.createElement('option');
    opt.value = h;
    opt.textContent = h;
    colSelect.appendChild(opt);
  });

  badge.textContent = `${tableData.total_rows} rows`;

  function renderRows(data) {
    tbody.innerHTML = '';
    data.forEach(row => {
      const tr = document.createElement('tr');
      headers.forEach((h, i) => {
        const td = document.createElement('td');
        const val = row[i];
        td.textContent = val === null || val === undefined ? '' : val;
        // Colour numeric cells based on column name hints
        const hn = h.toLowerCase();
        if ((hn.includes('spsf') || hn.includes('doi') || hn.includes('sell_thru') || hn.includes('pct')) && typeof val === 'number') {
          td.className = val < 0 || (hn.includes('spsf') && val < 500) ? 'p1-val' : '';
        }
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
  }
  renderRows(rows);

  function applyFilter() {
    const q = filterInput.value.toLowerCase();
    const col = colSelect.value;
    const colIdx = col ? headers.indexOf(col) : -1;
    const filtered = rows.filter(row => {
      if (colIdx >= 0) return String(row[colIdx] ?? '').toLowerCase().includes(q);
      return row.some(cell => String(cell ?? '').toLowerCase().includes(q));
    });
    renderRows(filtered);
    badge.textContent = `${filtered.length} / ${tableData.total_rows} rows`;
  }

  filterInput.addEventListener('input', applyFilter);
  colSelect.addEventListener('change', applyFilter);
}

function sortTable(tbody, rows, headers, colIdx, thEl) {
  const allTh = thEl.closest('thead').querySelectorAll('th');
  const asc = !thEl.classList.contains('sort-asc');
  allTh.forEach(t => t.classList.remove('sort-asc','sort-desc'));
  thEl.classList.add(asc ? 'sort-asc' : 'sort-desc');
  const sorted = [...rows].sort((a, b) => {
    const av = a[colIdx], bv = b[colIdx];
    const na = parseFloat(av), nb = parseFloat(bv);
    const cmp = isNaN(na)||isNaN(nb) ? String(av??'').localeCompare(String(bv??'')) : na - nb;
    return asc ? cmp : -cmp;
  });
  tbody.innerHTML = '';
  sorted.forEach(row => {
    const tr = document.createElement('tr');
    headers.forEach((_, i) => {
      const td = document.createElement('td');
      td.textContent = row[i] === null || row[i] === undefined ? '' : row[i];
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
}

// ─── Chart ────────────────────────────────────────────────────────────────────
function renderChart(el, chartData) {
  if (!chartData || !chartData.labels.length) return;

  const canvas = el.querySelector('.chart-canvas');
  const typeSelect = el.querySelector('.chart-type-select');
  const desc = el.querySelector('.chart-desc');
  let currentChart = null;

  const colors = ['rgba(0,200,255,.8)','rgba(255,140,0,.8)','rgba(0,220,130,.8)','rgba(255,59,59,.8)','rgba(167,139,250,.8)'];

  function buildChart(type) {
    if (currentChart) currentChart.destroy();
    const ctx = canvas.getContext('2d');
    currentChart = new Chart(ctx, {
      type: type,
      data: {
        labels: chartData.labels,
        datasets: (chartData.datasets || []).map((ds, i) => ({
          ...ds,
          backgroundColor: colors[i % colors.length],
          borderColor: colors[i % colors.length].replace('.8','1'),
          borderWidth: 2,
        })),
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {legend:{labels:{color:'#8a92a8',font:{size:11}}}},
        scales: type !== 'pie' && type !== 'doughnut' ? {
          x:{ticks:{color:'#8a92a8',font:{size:10},maxRotation:45},grid:{color:'rgba(42,47,64,.5)'}},
          y:{ticks:{color:'#8a92a8',font:{size:10}},grid:{color:'rgba(42,47,64,.5)'}},
        } : {},
      },
    });
    state.charts.set(canvas, currentChart);
  }

  buildChart(chartData.type || 'bar');
  if (desc) desc.textContent = `${chartData.label_column || ''} × ${(chartData.metric_columns || []).join(', ')}`;
  typeSelect.value = chartData.type || 'bar';
  typeSelect.addEventListener('change', () => buildChart(typeSelect.value));
}

// ─── SQL ──────────────────────────────────────────────────────────────────────
function renderSQL(el, sql) {
  if (!sql) return;
  const code = el.querySelector('.sql-code');
  const meta = el.querySelector('.sql-meta');
  const tables = el.querySelector('.sql-tables-info');
  const copy = el.querySelector('.btn-copy-sql');

  if (code) code.textContent = sql.sql || '';
  if (meta) meta.textContent = `${sql.row_count||0} rows · ${sql.execution_time_ms||0}ms`;
  if (tables) tables.textContent = 'Tables: ' + (sql.tables_used || []).join(', ');
  if (copy) copy.onclick = () => {
    navigator.clipboard.writeText(sql.sql || '');
    copy.textContent = '✓ Copied';
    setTimeout(() => copy.textContent = 'Copy SQL', 2000);
  };
}

// ─── Alert Strip ─────────────────────────────────────────────────────────────
function renderAlertStrip(el, alerts) {
  if (!alerts.length) return;
  const strip = el.querySelector('.alert-strip');
  alerts.slice(0, 3).forEach(a => {
    const chip = document.createElement('div');
    chip.className = `alert-chip ${(a.priority||'').toLowerCase()}`;
    chip.textContent = a.exception_text || `${a.priority} — ${a.kpi_type}`;
    strip.appendChild(chip);
  });
  if (alerts.length > 3) {
    const chip = document.createElement('div');
    chip.className = 'alert-chip';
    chip.textContent = `+${alerts.length - 3} more in Exception Inbox`;
    strip.appendChild(chip);
  }
}

// ─── Tab Delegation ───────────────────────────────────────────────────────────
function bindTabDelegation() {
  document.getElementById('chat-container').addEventListener('click', e => {
    const btn = e.target.closest('.tab-btn');
    if (!btn) return;
    const container = btn.closest('.response-tabs');
    activateTab(container.closest('.assistant-msg'), btn.dataset.tab);
  });
}

// ─── Exception Inbox ─────────────────────────────────────────────────────────
function bindExceptionFilters() {
  document.querySelectorAll('.exc-filter').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.exc-filter').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      state.activeAlertFilter = btn.dataset.priority;
      renderExceptionInbox();
    });
  });
}

async function loadAlerts() {
  try {
    const res = await fetch(`${API}/alerts?limit=50`);
    const data = await res.json();
    state.allAlerts = data.alerts || [];
    renderExceptionInbox();
    updateAlertBadge();
  } catch (e) {}
}

async function triggerAlertScan() {
  const btn = document.getElementById('btn-scan-alerts');
  if (!btn) return;
  btn.textContent = '...';
  btn.disabled = true;
  try {
    const res = await fetch(`${API}/alerts/scan`, { method: 'POST' });
    const data = await res.json();
    if (data.error) {
      btn.textContent = 'Err';
      setTimeout(() => { btn.textContent = 'Scan'; btn.disabled = false; }, 3000);
      return;
    }
    // Reload alerts from DB and update badge
    await loadAlerts();
    const total = (data.p1 || 0) + (data.p2 || 0) + (data.p3 || 0);
    btn.textContent = `+${data.alerts_saved}`;
    setTimeout(() => { btn.textContent = 'Scan'; btn.disabled = false; }, 2500);
  } catch (e) {
    btn.textContent = 'Scan';
    btn.disabled = false;
  }
}

document.addEventListener('DOMContentLoaded', () => {
  const scanBtn = document.getElementById('btn-scan-alerts');
  if (scanBtn) scanBtn.addEventListener('click', triggerAlertScan);
});

function renderExceptionInbox() {
  const list = document.getElementById('exception-list');
  const f = state.activeAlertFilter;
  let alerts = state.allAlerts.filter(a => !a.resolved);
  if (f !== 'all') alerts = alerts.filter(a => a.priority === f);
  list.innerHTML = '';
  if (!alerts.length) { list.innerHTML = '<div class="exc-empty">No active exceptions</div>'; return; }
  alerts.slice(0, 20).forEach(alert => {
    const p = (alert.priority||'P4').toLowerCase();
    const item = document.createElement('div');
    item.className = `exc-item ${p}`;
    item.innerHTML = `
      <div class="exc-item-header">
        <span class="exc-priority ${p}">${alert.priority}</span>
        <span class="exc-kpi">${alert.kpi_type||''}</span>
      </div>
      <div class="exc-text">${alert.exception_text||alert.dimension_value||''}</div>
    `;
    item.title = `Action: ${alert.recommended_action||''}`;
    item.addEventListener('click', () => resolveAlert(alert.alert_id, item));
    list.appendChild(item);
  });
}

async function resolveAlert(id, el) {
  try {
    await fetch(`${API}/alerts/${id}/resolve`, {method:'PATCH'});
    state.allAlerts = state.allAlerts.filter(a => a.alert_id !== id);
    el.style.opacity = '0.3';
    setTimeout(() => el.remove(), 500);
    updateAlertBadge();
  } catch (e) {}
}

function updateAlertBadge() {
  const n = state.allAlerts.filter(a => !a.resolved).length;
  const badge = document.getElementById('alert-count');
  badge.textContent = n;
  badge.style.display = n > 0 ? 'inline' : 'none';
}

// ─── KPI Dashboard ────────────────────────────────────────────────────────────
async function loadKPIDashboard() {
  try {
    // Load thresholds + alert counts
    const res = await fetch(`${API}/kpi/riect`);
    const data = await res.json();
    const s = data.alert_summary || {};
    const total = s.total || 0;
    const kpiAlerts = data.kpi_alerts || {};
    const thresh = data.kpi_thresholds || {};

    // Alert badge
    const alertBadge = document.getElementById('alert-count');
    if (alertBadge) {
      alertBadge.textContent = total;
      alertBadge.style.display = total > 0 ? 'inline' : 'none';
    }

    // Populate KPI cards with thresholds and alert counts
    const sp = thresh.SPSF || {};
    const st = thresh.SELL_THRU || {};
    const doi = thresh.DOI || {};
    const mbq = thresh.MBQ || {};

    renderKPICard('kpi-spsf', 'kpi-spsf-val', 'kpi-spsf-bar',
      `Target ₹${sp.target || 1200}`, `P1 < ₹${sp.P1 || 500}`,
      kpiAlerts.SPSF || {}, null, null);

    renderKPICard('kpi-sellthru', 'kpi-sellthru-val', 'kpi-sellthru-bar',
      `Target ${st.target || 100}%`, `P1 < ${st.P1 || 60}%`,
      kpiAlerts.SELL_THRU || {}, null, null);

    renderKPICard('kpi-doi', 'kpi-doi-val', 'kpi-doi-bar',
      `Target ${doi.target || 15}d`, `P1 > ${doi.P1 || 90}d`,
      kpiAlerts.DOI || {}, null, null);

    renderKPICard('kpi-mbq', 'kpi-mbq-val', 'kpi-mbq-bar',
      `P2 < ${mbq.high_shortfall_pct || 75}%`, `P1 < ${mbq.critical_shortfall_pct || 50}%`,
      kpiAlerts.MBQ || {}, null, null);

    // Then try to fetch live computed KPI values from ClickHouse
    loadKPILive(thresh);

  } catch (e) { console.warn('KPI dashboard load failed:', e); }
}

async function loadKPILive(thresh) {
  try {
    const res = await fetch(`${API}/kpi/live`);
    const data = await res.json();
    if (data.status !== 'live' || !data.chain) return;

    const chain = data.chain;
    const sp  = (thresh || {}).SPSF     || {};
    const doi = (thresh || {}).DOI      || {};
    const mbq = (thresh || {}).MBQ      || {};

    // SPSF — monthly projected (main) + daily running (sub)
    const dailySpsf    = chain.spsf_daily    ?? chain.spsf;
    const monthlySpsf  = chain.spsf_monthly_projected;
    if (dailySpsf != null) {
      const target    = sp.target || 1000;
      const refSpsf   = monthlySpsf ?? dailySpsf;
      const spsfPct   = Math.min(refSpsf / target * 100, 100);
      const spsfP     = refSpsf < (sp.P1 || 500) ? 'P1' :
                        refSpsf < (sp.P2 || 750) ? 'P2' :
                        refSpsf < (sp.P3 || 1000) ? 'P3' : 'OK';

      const mainLabel = monthlySpsf != null
        ? `₹${monthlySpsf.toLocaleString('en-IN', {maximumFractionDigits:0})} /sqft`
        : `₹${dailySpsf.toFixed(1)} /sqft`;

      renderKPICard('kpi-spsf', 'kpi-spsf-val', 'kpi-spsf-bar',
        mainLabel,
        monthlySpsf != null
          ? `Monthly proj · Target ₹${target.toLocaleString('en-IN')}`
          : `Daily · Target ₹${target.toLocaleString('en-IN')}`,
        {}, spsfP, spsfPct);

      // Sub2: daily running + store count
      const sub2El = document.getElementById('kpi-spsf-sub2');
      if (sub2El) {
        const daysLabel = data.days_elapsed && data.days_in_month
          ? ` · Day ${data.days_elapsed}/${data.days_in_month}`
          : '';
        sub2El.textContent = monthlySpsf != null
          ? `Daily ₹${dailySpsf.toFixed(1)} · ${chain.store_count} stores${daysLabel}`
          : `${chain.store_count} stores${daysLabel}`;
      }
    }

    // Sell-Through — % only (no amounts), monthly projected (main) + running MTD (sub)
    const stRunning = chain.sell_thru_pct_running;
    const stMonthly = chain.sell_thru_pct_monthly;
    if (stRunning != null || stMonthly != null) {
      const stTarget = (thresh || {}).SELL_THRU ? ((thresh.SELL_THRU.target) || 95) : 95;
      const stP1     = (thresh || {}).SELL_THRU ? ((thresh.SELL_THRU.P1)    || 60) : 60;
      const stP2     = (thresh || {}).SELL_THRU ? ((thresh.SELL_THRU.P2)    || 80) : 80;
      const stP3     = (thresh || {}).SELL_THRU ? ((thresh.SELL_THRU.P3)    || 95) : 95;

      const refSt  = stMonthly ?? stRunning;
      const stPct  = Math.min(refSt / stTarget * 100, 100);
      const stP    = refSt < stP1 ? 'P1' : refSt < stP2 ? 'P2' : refSt < stP3 ? 'P3' : 'OK';

      const mainSt = stMonthly != null
        ? `${stMonthly.toFixed(1)}%`
        : `${stRunning.toFixed(1)}%`;

      renderKPICard('kpi-sellthru', 'kpi-sellthru-val', 'kpi-sellthru-bar',
        mainSt,
        stMonthly != null
          ? `Monthly proj · Target ${stTarget}%`
          : `MTD running · Target ${stTarget}%`,
        {}, stP, stPct);

      // Sub2: MTD running % + inventory date
      const sub2St = document.getElementById('kpi-sellthru-sub2');
      if (sub2St) {
        const invLabel = chain.latest_inv_date ? ` · Inv ${chain.latest_inv_date}` : '';
        sub2St.textContent = stMonthly != null && stRunning != null
          ? `MTD running ${stRunning.toFixed(1)}%${invLabel}`
          : invLabel.trim() || 'Inventory data';
      }
    } else if (chain.net_sales != null) {
      // Fallback: inventory not available — show bills + UPT (no amounts per requirement)
      renderKPICard('kpi-sellthru', 'kpi-sellthru-val', 'kpi-sellthru-bar',
        '—',
        `${chain.total_bills.toLocaleString('en-IN')} bills · UPT ${chain.upt || '—'}`,
        {}, null, null);
    }

    // DOI — show days of inventory (from live KPI calc) + ATV sub
    if (chain.doi_days != null) {
      const doiColor = chain.doi_days > (doi.P1 || 90) ? 'p1-val'
                     : chain.doi_days > (doi.P2 || 60) ? 'p2-val'
                     : chain.doi_days > (doi.P3 || 30) ? 'p3-val' : '';
      renderKPICard('kpi-doi', 'kpi-doi-val', 'kpi-doi-bar',
        `${chain.doi_days.toFixed(0)} Days`,
        `ATV ₹${chain.atv ? chain.atv.toLocaleString('en-IN') : '—'} · UPT ${chain.upt || '—'}`,
        {}, null, null);
      const doiValEl = document.getElementById('kpi-doi-val');
      if (doiValEl && doiColor) doiValEl.className = 'kpi-value ' + doiColor;
    } else if (chain.total_qty != null) {
      // Fallback if doi_days not available
      renderKPICard('kpi-doi', 'kpi-doi-val', 'kpi-doi-bar',
        `${chain.total_qty.toLocaleString('en-IN')} units`,
        `ATV ₹${chain.atv ? chain.atv.toLocaleString('en-IN') : '—'} · Target ${doi.target || 15}d`,
        {}, null, null);
    }

    // MBQ — show Discount Rate
    if (chain.disc_rate_pct != null) {
      renderKPICard('kpi-mbq', 'kpi-mbq-val', 'kpi-mbq-bar',
        `${chain.disc_rate_pct}% disc`,
        `P1 < ${mbq.critical_shortfall_pct || 50}% compliance`,
        {}, null, null);
    }

    // Update date label under KPI grid
    if (data.latest_date) {
      let chip = document.querySelector('.kpi-date-chip');
      if (!chip) {
        chip = document.createElement('div');
        chip.className = 'kpi-date-chip';
        const grid = document.querySelector('.kpi-grid');
        if (grid) grid.insertAdjacentElement('afterend', chip);
      }
      chip.textContent = `Data as of ${data.latest_date}`;
    }

  } catch (e) { /* ClickHouse may be unavailable — silent fail */ }
}

function renderKPICard(cardId, valId, barId, mainVal, subVal, alerts, priority, barPct) {
  const card  = document.getElementById(cardId);
  const valEl = document.getElementById(valId);
  const barEl = document.getElementById(barId);
  if (!card || !valEl) return;

  valEl.textContent = mainVal;

  // Sub-label: show alert count if any, else threshold info
  const subEl = card.querySelector('.kpi-sub');
  const p1cnt = alerts.P1 || 0;
  const p2cnt = alerts.P2 || 0;
  if (subEl) {
    if (p1cnt > 0) subEl.innerHTML = `<span style="color:#FF3B3B">⚠ ${p1cnt} P1</span>`;
    else if (p2cnt > 0) subEl.innerHTML = `<span style="color:#FF8C00">▲ ${p2cnt} P2</span>`;
    else subEl.textContent = subVal;
  }

  // Priority colour on card
  card.classList.remove('p1','p2','p3','p4','ok');
  if (priority === 'P1') card.classList.add('p1');
  else if (priority === 'P2') card.classList.add('p2');
  else if (priority === 'P3') card.classList.add('p3');

  // Progress bar
  if (barEl && barPct != null) {
    barEl.style.width = Math.max(2, Math.min(100, barPct)) + '%';
    barEl.style.backgroundColor =
      priority === 'P1' ? '#FF3B3B' :
      priority === 'P2' ? '#FF8C00' :
      priority === 'P3' ? '#FFD700' : 'var(--accent)';
  }
}

function updateKPIFromSummary(kpi) {
  // Called from WebSocket kpi_summary event — update cards with live computed values
  const map = {
    spsf:         {card:'kpi-spsf',     val:'kpi-spsf-val',     bar:'kpi-spsf-bar'},
    sell_thru_pct:{card:'kpi-sellthru', val:'kpi-sellthru-val', bar:'kpi-sellthru-bar'},
    doi:          {card:'kpi-doi',      val:'kpi-doi-val',      bar:'kpi-doi-bar'},
    mbq:          {card:'kpi-mbq',      val:'kpi-mbq-val',      bar:'kpi-mbq-bar'},
  };
  Object.entries(map).forEach(([key, ids]) => {
    const v = kpi[key];
    if (v == null) return;
    const valEl = document.getElementById(ids.val);
    if (valEl) {
      if (key === 'sell_thru_pct') valEl.textContent = (v * 100).toFixed(1) + '%';
      else if (key === 'doi')      valEl.textContent = v.toFixed(1) + 'd';
      else if (key === 'spsf')     valEl.textContent = '₹' + v.toLocaleString('en-IN', {maximumFractionDigits:0});
      else                          valEl.textContent = v;
    }
  });
  if ((kpi.total_p1||0) > 0) {
    ['kpi-spsf','kpi-sellthru','kpi-doi','kpi-mbq'].forEach(id => {
      const card = document.getElementById(id);
      if (card && !card.classList.contains('p1')) card.classList.add('p1');
    });
  }
}

// ─── Settings ─────────────────────────────────────────────────────────────────
async function loadSettings() {
  try {
    const [chRes, llmRes, provRes] = await Promise.all([
      fetch(`${API}/settings/clickhouse`),
      fetch(`${API}/settings/llm`),
      fetch(`${API}/auth/providers`),
    ]);
    const ch = await chRes.json();
    const llm = await llmRes.json();
    const prov = await provRes.json();

    // Only populate if user has actually saved credentials (not defaults)
    if (ch.host) document.getElementById('ch-host').value = ch.host;
    if (ch.user) document.getElementById('ch-user').value = ch.user;
    if (ch.port) document.getElementById('ch-port').value = ch.port;
    if (ch.schemas) document.getElementById('ch-schemas').value = ch.schemas.join ? ch.schemas.join(',') : ch.schemas;

    // Show configured/unconfigured status
    const clearBtn = document.getElementById('ch-clear-btn');
    const badge = document.getElementById('ch-configured-badge');
    if (ch.configured) {
      showChStatus('success', `✓ Saved — click Test Connection to verify`);
      if (clearBtn) clearBtn.style.display = '';
      if (badge) badge.style.display = '';
    } else {
      showChStatus('', 'Enter your ClickHouse credentials and click Test Connection.');
      if (clearBtn) clearBtn.style.display = 'none';
      if (badge) badge.style.display = 'none';
    }

    // Ollama status in settings
    const dot = document.getElementById('ollama-dot');
    const text = document.getElementById('ollama-text');
    if (llm.ollama_models && llm.ollama_models.length > 0) {
      dot.className = 'status-dot connected';
      text.textContent = `${llm.ollama_models.length} models available · using ${llm.best_ollama_model || ''}`;
    } else {
      dot.className = 'status-dot error';
      text.textContent = 'Ollama not running — install from ollama.ai';
    }

    // Populate model selector in settings
    const sel = document.getElementById('default-llm');
    sel.innerHTML = '<option value="auto">Auto (best available)</option>';
    (llm.ollama_models || []).forEach(m => {
      const opt = document.createElement('option');
      opt.value = m; opt.textContent = m;
      if (m === llm.best_ollama_model) opt.selected = true;
      sel.appendChild(opt);
    });
    ['claude','gemini','openai'].forEach(p => {
      if (prov[p]?.connected) {
        const opt = document.createElement('option');
        opt.value = p; opt.textContent = p.charAt(0).toUpperCase() + p.slice(1);
        sel.appendChild(opt);
      }
    });

    // Provider card status — pass auth_type so card shows correct label
    updateProviderCard('claude',  prov.claude?.connected,  null, prov.claude?.auth_type);
    updateProviderCard('gemini',  prov.gemini?.connected,  prov.gemini?.email || null, prov.gemini?.auth_type);
    updateProviderCard('openai',  prov.openai?.connected,  null, prov.openai?.auth_type);

    // Qwen provider card
    await updateQwenStatus();

    // Load stored Google Client ID
    const cidRes = await fetch(`${API}/auth/google/client_id`);
    const cid = await cidRes.json();
    if (cid.client_id) {
      const el = document.getElementById('google-client-id-input');
      if (el) el.value = cid.client_id;
    }

  } catch (e) { console.warn('Settings load failed:', e); }
}

function updateProviderCard(provider, isConnected, detail, authType) {
  const card = document.getElementById(`card-${provider}`);
  const actions = document.getElementById(`${provider}-actions`);
  if (!card || !actions) return;

  card.classList.toggle('connected', isConnected);
  if (isConnected) {
    // Show "Connected via Account" when auth comes from env/subscription,
    // "Connected" when user manually configured an API key
    const connLabel = authType === 'account'
      ? '✓ Connected via Account'
      : '✓ Connected';
    const detailStr = detail ? `<span class="connected-detail">${detail}</span>` : '';
    // Only show Disconnect button for manually-configured keys (not system env vars)
    const disconnectBtn = authType === 'account'
      ? ''
      : `<button class="btn-disconnect" onclick="disconnectProvider('${provider}')">Disconnect</button>`;
    actions.innerHTML = `<span class="connected-badge">${connLabel}</span>${detailStr}${disconnectBtn}`;
    const form = document.getElementById(`${provider}-form`);
    if (form) form.style.display = 'none';
  } else {
    actions.innerHTML = `<button class="btn-connect" onclick="connectAccount('${provider}')">Connect Account</button>`;
  }
}

function connectAccount(provider) {
  const form = document.getElementById(`${provider}-form`);
  if (form) form.style.display = form.style.display === 'none' ? '' : 'none';
}

function openProviderPortal(provider) {
  const portals = {
    claude:  'https://console.anthropic.com/settings/keys',
    gemini:  'https://aistudio.google.com/app/apikey',
    openai:  'https://platform.openai.com/api-keys',
  };
  const url = portals[provider];
  if (url) window.open(url, `${provider}_portal`, 'width=900,height=700,left=100,top=80');
  setTimeout(() => {
    const inp = document.getElementById(`${provider}-key-input`);
    if (inp) inp.focus();
  }, 800);
}

function openAndWait(provider) {
  const portals = {
    claude:  'https://console.anthropic.com/settings/keys',
    gemini:  'https://aistudio.google.com/app/apikey',
    openai:  'https://platform.openai.com/api-keys',
  };
  const url = portals[provider];
  if (!url) return;
  window.open(url, `${provider}_auth`, 'width=960,height=700,left=80,top=60');
  showProviderMsg(provider, '⏳ Logging in… return here and paste your token below.', '');
  // When user returns to this window, focus the paste input
  function onFocus() {
    window.removeEventListener('focus', onFocus);
    setTimeout(() => {
      const inp = document.getElementById(`${provider}-key-input`);
      if (inp) { inp.focus(); inp.select(); }
    }, 300);
  }
  window.addEventListener('focus', onFocus);
}

async function saveAccountKey(provider) {
  const input = document.getElementById(`${provider}-key-input`);
  const key = input ? input.value.trim() : '';
  const statusEl = document.getElementById(`${provider}-status`);

  if (!key) {
    if (statusEl) { statusEl.textContent = 'Please enter an API key'; statusEl.className = 'provider-status-msg error'; }
    return;
  }

  if (statusEl) { statusEl.textContent = 'Saving...'; statusEl.className = 'provider-status-msg'; }
  try {
    const res = await fetch(`${API}/settings/llm/${provider}`, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({key}),
    });
    if (!res.ok) throw new Error(await res.text());
    input.value = '';
    updateProviderCard(provider, true, null);
    loadStatus();
    if (statusEl) statusEl.textContent = '';
  } catch (e) {
    if (statusEl) { statusEl.textContent = `Error: ${e.message}`; statusEl.className = 'provider-status-msg error'; }
  }
}

async function disconnectProvider(provider) {
  try {
    if (provider === 'gemini') {
      await fetch(`${API}/auth/google`, {method: 'DELETE'});
    } else {
      await fetch(`${API}/settings/llm/${provider}`, {method: 'DELETE'});
    }
    updateProviderCard(provider, false, null);
    loadStatus();
  } catch (e) { console.warn('Disconnect failed:', e); }
}

// ─── Qwen Account Auth ─────────────────────────────────────────────────────────

function toggleQwenForm() {
  const form = document.getElementById('qwen-form');
  if (!form) return;
  form.style.display = form.style.display === 'none' ? '' : 'none';
}

function qwenSwitchTab(tab) {
  document.getElementById('qwen-panel-email').style.display = tab === 'email' ? '' : 'none';
  document.getElementById('qwen-panel-token').style.display = tab === 'token' ? '' : 'none';
  document.getElementById('qwen-tab-email').classList.toggle('active', tab === 'email');
  document.getElementById('qwen-tab-token').classList.toggle('active', tab === 'token');
}


async function qwenSaveToken() {
  const token   = (document.getElementById('qwen-token-input')?.value || '').trim();
  const statusEl = document.getElementById('qwen-status');
  const btn      = document.getElementById('qwen-token-btn');

  if (!token) {
    if (statusEl) { statusEl.textContent = 'Paste your session token first'; statusEl.className = 'provider-status-msg error'; }
    return;
  }

  if (statusEl) { statusEl.textContent = '⏳ Verifying token...'; statusEl.className = 'provider-status-msg'; }
  if (btn) btn.disabled = true;

  try {
    const res  = await fetch(`${API}/settings/qwen/token`, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({token}),
    });
    const data = await res.json();

    if (res.ok && data.status === 'connected') {
      if (statusEl) { statusEl.textContent = `✓ Connected as ${data.email}`; statusEl.className = 'provider-status-msg success'; }
      document.getElementById('qwen-token-input').value = '';
      updateQwenCard(true, data.email, null);
      await loadStatus();
    } else {
      const msg = data.detail || data.message || 'Token invalid or expired';
      if (statusEl) { statusEl.textContent = `✗ ${msg}`; statusEl.className = 'provider-status-msg error'; }
    }
  } catch (e) {
    if (statusEl) { statusEl.textContent = `✗ Error: ${e.message}`; statusEl.className = 'provider-status-msg error'; }
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function qwenLogin() {
  const email    = (document.getElementById('qwen-email-input')?.value || '').trim();
  const password = (document.getElementById('qwen-password-input')?.value || '').trim();
  const statusEl = document.getElementById('qwen-status');
  const btn      = document.getElementById('qwen-login-btn');

  if (!email || !password) {
    if (statusEl) { statusEl.textContent = 'Enter email and password'; statusEl.className = 'provider-status-msg error'; }
    return;
  }

  if (statusEl) { statusEl.textContent = '⏳ Connecting to Qwen...'; statusEl.className = 'provider-status-msg'; }
  if (btn) btn.disabled = true;

  try {
    const res  = await fetch(`${API}/settings/qwen/login`, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({email, password}),
    });
    const data = await res.json();

    if (res.ok && data.status === 'connected') {
      if (statusEl) { statusEl.textContent = `✓ Connected as ${data.email}`; statusEl.className = 'provider-status-msg success'; }
      document.getElementById('qwen-password-input').value = '';
      const hint = document.getElementById('qwen-google-hint');
      if (hint) hint.style.display = 'none';
      updateQwenCard(true, data.email, data.model);
      await loadStatus();
    } else {
      const msg = data.detail || data.message || 'Login failed';
      if (statusEl) { statusEl.textContent = `✗ ${msg}`; statusEl.className = 'provider-status-msg error'; }
      // Show Google-account hint if account not found (Google-linked account has no password)
      const hint = document.getElementById('qwen-google-hint');
      if (hint && msg.toLowerCase().includes('not found')) hint.style.display = '';
    }
  } catch (e) {
    if (statusEl) { statusEl.textContent = `✗ Error: ${e.message}`; statusEl.className = 'provider-status-msg error'; }
  } finally {
    if (btn) btn.disabled = false;
  }
}

async function qwenSetModel(modelId) {
  try {
    await fetch(`${API}/settings/qwen/model`, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({model: modelId}),
    });
    const statusEl = document.getElementById('qwen-status');
    if (statusEl) { statusEl.textContent = `✓ Model set to ${modelId}`; statusEl.className = 'provider-status-msg success'; }
    await loadStatus();
  } catch (e) { console.warn('Qwen set model failed:', e); }
}

async function qwenDisconnect() {
  try {
    await fetch(`${API}/settings/qwen`, {method: 'DELETE'});
    updateQwenCard(false, null, null);
    await loadStatus();
  } catch (e) { console.warn('Qwen disconnect failed:', e); }
}

function updateQwenCard(isConnected, email, model) {
  const card    = document.getElementById('card-qwen');
  const actions = document.getElementById('qwen-actions');
  const form    = document.getElementById('qwen-form');
  const statusEl = document.getElementById('qwen-status');
  if (!card || !actions) return;

  card.classList.toggle('connected', isConnected);
  if (isConnected) {
    const modelLabel = model || 'Qwen3.5-Plus';
    const emailSpan  = email ? `<span class="connected-detail">${email}</span>` : '';
    actions.innerHTML = `
      <span class="connected-badge">✓ Connected</span>
      ${emailSpan}
      <button class="btn-disconnect" onclick="qwenDisconnect()">Disconnect</button>`;
    if (form) form.style.display = 'none';
    // Update model selector to reflect active model
    const sel = document.getElementById('qwen-model-select');
    if (sel && model) sel.value = model;
    if (statusEl) { statusEl.textContent = `Active model: ${modelLabel}`; statusEl.className = 'provider-status-msg success'; }
  } else {
    actions.innerHTML = `<button class="btn-connect" onclick="toggleQwenForm()">Connect Account</button>`;
    if (form) form.style.display = 'none';
    if (statusEl) { statusEl.textContent = ''; statusEl.className = 'provider-status-msg'; }
  }
}

async function updateQwenStatus() {
  try {
    const res  = await fetch(`${API}/settings/qwen/status`);
    const data = await res.json();
    updateQwenCard(data.connected, data.email, data.model);
  } catch (e) { console.warn('Qwen status load failed:', e); }
}

// Poll interval handle for qwenConnect
let _qwenPollTimer = null;

// Bookmarklet: reads Qwen token from localStorage and navigates to RIECT capture endpoint.
// Navigation (not fetch) bypasses HTTPS→HTTP mixed-content blocking.
const _QWEN_BOOKMARKLET = `javascript:(function(){` +
  `var keys=['token','access_token','Authorization','id_token','authToken','QWEN_TOKEN','userToken'];` +
  `var t=null;` +
  `for(var i=0;i<keys.length;i++){var v=localStorage.getItem(keys[i]);if(v&&v.length>20){t=v;break;}}` +
  `if(!t){for(var k in localStorage){var v=localStorage.getItem(k);` +
  `if(v&&typeof v==='string'&&v.length>50){t=v;break;}}}` +
  `if(t){window.location.href='http://localhost:8001/qwen-connect?token='+encodeURIComponent(t);}` +
  `else{alert('Sign in to chat.qwen.ai first, then click this bookmark.');}` +
  `})();`;

async function qwenConnect() {
  const btn      = document.getElementById('qwen-connect-btn');
  const instr    = document.getElementById('qwen-instructions');
  const statusEl = document.getElementById('qwen-status');
  const bmkEl    = document.getElementById('qwen-bookmarklet');

  // Set bookmarklet href on the draggable anchor
  if (bmkEl) bmkEl.href = _QWEN_BOOKMARKLET;

  // Open chat.qwen.ai in a new tab
  window.open('https://chat.qwen.ai', '_blank');

  // Show step-by-step instructions
  if (instr) instr.style.display = '';
  if (btn)   { btn.textContent = '⏳ Waiting for connection…'; btn.disabled = true; }
  if (statusEl) {
    statusEl.textContent = 'Sign in on Qwen tab → click the bookmark → this page connects automatically';
    statusEl.className = 'provider-status-msg';
  }

  // Poll every 3 s — auto-detects when /qwen-connect saves the token
  if (_qwenPollTimer) clearInterval(_qwenPollTimer);
  _qwenPollTimer = setInterval(async () => {
    try {
      const res  = await fetch(`${API}/settings/qwen/status`);
      const data = await res.json();
      if (data.connected) {
        clearInterval(_qwenPollTimer);
        _qwenPollTimer = null;
        updateQwenCard(true, data.email, data.model);
        if (btn)   { btn.textContent = '✦ Sign in with Qwen & Connect'; btn.disabled = false; }
        if (instr) instr.style.display = 'none';
        await loadStatus();
      }
    } catch (_) {}
  }, 3000);
}

// ─── Google OAuth2 PKCE ────────────────────────────────────────────────────────

function toggleClientIdSetup() {
  const el = document.getElementById('gemini-client-id-setup');
  if (el) el.style.display = el.style.display === 'none' ? '' : 'none';
}

async function saveGoogleClientId() {
  const input = document.getElementById('google-client-id-input');
  const cid = input ? input.value.trim() : '';
  if (!cid) return;
  try {
    await fetch(`${API}/auth/google/client_id`, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({client_id: cid}),
    });
    document.getElementById('gemini-client-id-setup').style.display = 'none';
    document.querySelector('#gemini-client-id-row .oauth-note').textContent = 'Google OAuth Client ID configured · ';
  } catch (e) { console.error('Save client ID failed:', e); }
}

async function startGoogleOAuth() {
  // Get stored Client ID
  let clientId = '';
  try {
    const r = await fetch(`${API}/auth/google/client_id`);
    const d = await r.json();
    clientId = d.client_id || '';
  } catch (e) {}

  if (!clientId) {
    // Show the Client ID setup section
    const setup = document.getElementById('gemini-client-id-setup');
    if (setup) setup.style.display = '';
    const inp = document.getElementById('google-client-id-input');
    if (inp) inp.focus();
    showProviderMsg('gemini', 'Please configure your Google OAuth Client ID first', 'error');
    return;
  }

  const redirectUri = `${location.origin}/api/auth/google/callback`;
  try {
    showProviderMsg('gemini', '⏳ Opening Google login...', '');
    const res = await fetch(`${API}/auth/google/start`, {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({client_id: clientId, redirect_uri: redirectUri}),
    });
    const data = await res.json();
    const popup = window.open(data.auth_url, 'google_auth', 'width=520,height=640,left=200,top=100');

    // Listen for postMessage callback from OAuth callback page
    window.addEventListener('message', async function handler(e) {
      if (e.origin !== location.origin) return;
      if (!e.data || e.data.type !== 'oauth_code') return;
      window.removeEventListener('message', handler);
      popup && popup.close();

      try {
        showProviderMsg('gemini', '⏳ Completing sign-in...', '');
        const exRes = await fetch(`${API}/auth/google/exchange`, {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            code: e.data.code,
            code_verifier: '',
            client_id: clientId,
            redirect_uri: redirectUri,
          }),
        });
        const exData = await exRes.json();
        if (exData.status === 'connected') {
          updateProviderCard('gemini', true, exData.email || null);
          showProviderMsg('gemini', '', '');
          loadStatus();
        } else {
          showProviderMsg('gemini', 'Sign-in failed', 'error');
        }
      } catch (err) { showProviderMsg('gemini', `Error: ${err.message}`, 'error'); }
    });
  } catch (e) {
    showProviderMsg('gemini', `OAuth failed: ${e.message}`, 'error');
  }
}

function showProviderMsg(provider, msg, type) {
  const el = document.getElementById(`${provider}-status`);
  if (!el) return;
  el.textContent = msg;
  el.className = type ? `provider-status-msg ${type}` : 'provider-status-msg';
}

async function clearClickHouse() {
  try {
    await fetch(`${API}/settings/clickhouse`, {method: 'DELETE'});
    // Clear form fields
    document.getElementById('ch-host').value = '';
    document.getElementById('ch-user').value = '';
    document.getElementById('ch-pass').value = '';
    document.getElementById('ch-port').value = '8443';
    document.getElementById('ch-secure').checked = true;
    const clearBtn = document.getElementById('ch-clear-btn');
    const badge = document.getElementById('ch-configured-badge');
    if (clearBtn) clearBtn.style.display = 'none';
    if (badge) badge.style.display = 'none';
    showChStatus('', 'Config cleared. Enter new credentials and click Test Connection.');
  } catch (e) { showChStatus('error', `Error: ${e.message}`); }
}

// ClickHouse settings
async function saveClickHouse() {
  const cfg = _getChConfig();
  try {
    await fetch(`${API}/settings/clickhouse`, {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(cfg)});
    showChStatus('success', '✓ Saved');
  } catch (e) { showChStatus('error', e.message); }
}

async function testClickHouse() {
  const cfg = _getChConfig();
  showChStatus('', '⏳ Testing...');
  try {
    const res = await fetch(`${API}/settings/clickhouse/test`, {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(cfg)});
    const data = await res.json();
    if (data.status === 'connected') {
      const info = Object.entries(data.schemas||{}).map(([s,c]) => `${s}:${c}`).join(' · ');
      showChStatus('success', `✓ Connected · ${data.total_tables} tables — ${info}`);
      const clearBtn = document.getElementById('ch-clear-btn');
      const badge = document.getElementById('ch-configured-badge');
      if (clearBtn) clearBtn.style.display = '';
      if (badge) badge.style.display = '';
    } else {
      showChStatus('error', `✗ ${data.error}`);
    }
  } catch (e) { showChStatus('error', e.message); }
}

function _parseChHost(raw) {
  // If user pasted a full URL (http://host/path), extract just the hostname and derive port/secure
  raw = raw.trim();
  if (raw.startsWith('http://') || raw.startsWith('https://')) {
    try {
      const u = new URL(raw);
      const secure = u.protocol === 'https:';
      const port = u.port ? parseInt(u.port) : (secure ? 443 : 80);
      return { host: u.hostname, port, secure };
    } catch (e) { /* fall through */ }
  }
  return null; // plain hostname, leave port/secure as-is
}

function _getChConfig() {
  const rawHost = document.getElementById('ch-host').value.trim();
  const parsed  = _parseChHost(rawHost);
  return {
    host:     parsed ? parsed.host   : rawHost,
    port:     parsed ? parsed.port   : (parseInt(document.getElementById('ch-port').value) || 8443),
    user:     document.getElementById('ch-user').value.trim(),
    password: document.getElementById('ch-pass').value,
    secure:   parsed ? parsed.secure : document.getElementById('ch-secure').checked,
    schemas:  document.getElementById('ch-schemas').value.split(',').map(s => s.trim()),
  };
}

// Auto-parse URL pasted into host field and update port/secure fields
function onChHostChange() {
  const raw    = document.getElementById('ch-host').value;
  const parsed = _parseChHost(raw);
  if (parsed) {
    document.getElementById('ch-host').value   = parsed.host;
    document.getElementById('ch-port').value   = parsed.port;
    document.getElementById('ch-secure').checked = parsed.secure;
  }
}

function showChStatus(type, msg) {
  const el = document.getElementById('ch-status');
  el.className = type ? `conn-status ${type}` : 'conn-status';
  el.textContent = msg;
}

async function saveDefaultLLM() {
  const model = document.getElementById('default-llm').value;
  try {
    await fetch(`${API}/settings/llm/default`, {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({model})});
    document.getElementById('llm-selector').value = model;
  } catch (e) {}
}

// Settings open/close
document.getElementById('settings-btn').addEventListener('click', openSettings);
function openSettings() { document.getElementById('settings-panel').classList.add('open'); document.getElementById('settings-overlay').classList.add('open'); }
function closeSettings() { document.getElementById('settings-panel').classList.remove('open'); document.getElementById('settings-overlay').classList.remove('open'); }
document.getElementById('alert-bell').addEventListener('click', () => { document.querySelector('.ct-section:nth-child(2)')?.scrollIntoView({behavior:'smooth'}); });

// ─── Helpers ──────────────────────────────────────────────────────────────────
function showWelcome() {
  const container = document.getElementById('chat-container');
  if (document.getElementById('chat-welcome')) return;
  const div = document.createElement('div');
  div.id = 'chat-welcome';
  div.className = 'chat-welcome';
  div.innerHTML = `
    <div class="welcome-logo">DSR|RIECT</div>
    <div class="welcome-text">Retail Intelligence at your command</div>
    <div class="welcome-suggestions">
      <button class="suggestion-chip" onclick="sendSuggestion(this)">Show top 10 stores by SPSF</button>
      <button class="suggestion-chip" onclick="sendSuggestion(this)">Sell-through by category this week</button>
      <button class="suggestion-chip" onclick="sendSuggestion(this)">Stores with DOI above 60 days</button>
      <button class="suggestion-chip" onclick="sendSuggestion(this)">Peak hours customer footfall</button>
    </div>`;
  container.appendChild(div);
}

function bindInputEvents() {
  document.getElementById('chat-input').addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendMessage(); }
  });
}

function setStatus(st, text) {
  document.getElementById('ws-dot').className = `status-dot ${st}`;
  document.getElementById('status-text').textContent = text;
}

function updateStatus(text) { document.getElementById('status-text').textContent = text; }

function addPipelinePip(text, type) {
  if (!state.currentMsgEl) return;
  const header = state.currentMsgEl.querySelector('.msg-header');
  if (!header) return;
  const span = document.createElement('span');
  span.className = `pipeline-msg ${type}`;
  span.textContent = `· ${text}`;
  header.appendChild(span);
}

function endStreaming() {
  state.isStreaming = false;
  state.currentMsgEl = null;
  document.getElementById('send-btn').disabled = false;
  document.getElementById('chat-input').disabled = false;
  document.getElementById('chat-input').focus();
  setStatus('connected', 'Ready');
}

function scrollChat() {
  const c = document.getElementById('chat-container');
  c.scrollTop = c.scrollHeight;
}
