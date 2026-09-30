'use strict';

const $ = (s, el = document) => el.querySelector(s);
const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const time = (iso) => { const d = new Date(iso); return isNaN(d) ? '' : d.toLocaleTimeString('ro-RO'); };
const fmtBadge = (f) => f ? `<span class="fmt ${esc(f)}">${esc(f.toUpperCase())}</span>` : '';

async function api(method, url, body) {
  const r = await fetch(url, { method, headers: { 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
  const data = await r.json();
  if (!r.ok) throw new Error(data.error || r.statusText);
  return data;
}

/** Formatare lizibilă a mesajului exact de pe fir (JSON sau XML). */
function pretty(raw) {
  if (!raw) return '';
  const t = raw.trim();
  if (t.startsWith('{')) { try { return JSON.stringify(JSON.parse(t), null, 2); } catch { return raw; } }
  if (t.startsWith('<message>')) return t.replace(/<message>/, '<message>\n').replace(/(<\/[^>]+>)(?=<)/g, '$1\n').replace(/\n(?!<\/message>)(<)/g, '\n  $1');
  return raw;
}

// =============================================================== broker/stats
let lastCounters = {};
let lastTopicCounts = {};

function renderStats(s) {
  for (const dd of document.querySelectorAll('#counters dd')) {
    const k = dd.dataset.c, v = s.counters[k] ?? 0;
    if (lastCounters[k] !== undefined && lastCounters[k] !== v) { dd.classList.remove('bump'); void dd.offsetWidth; dd.classList.add('bump'); }
    dd.textContent = v;
  }
  lastCounters = s.counters;

  const b = s.broker;
  $('#cfg').textContent = `${b.transport} :${b.port}, ${b.workers} work-joburi, ACK în ${b.ack_timeout}s`;
  $('#storageMode').textContent = b.storage;

  // canale (subiecte)
  const topics = $('#topics');
  $('#topicList').innerHTML = s.topics.map((t) => `<option value="${esc(t.name)}">`).join('');
  if (!s.topics.length) {
    topics.innerHTML = '<li class="empty">Niciun canal încă. Publică un mesaj ca să apară primul.</li>';
  } else {
    topics.innerHTML = s.topics.map((t) => {
      const pending = t.waiting + t.backlog;
      const width = pending ? Math.max(8, Math.min(100, pending * 8)) : 0;
      const pulse = lastTopicCounts[t.name] !== undefined && lastTopicCounts[t.name] !== t.published;
      const state = t.backlog ? `${t.backlog} rețin. fără abonați` : t.waiting ? `${t.waiting} în așteptare` : `${t.subscribers} abonați`;
      return `<li class="pipe">
        <span class="t-name">${esc(t.name)}</span>
        <span class="tube${pulse ? ' pulse' : ''}" title="${pending} mesaje în așteptare"><span class="fill${t.backlog ? ' backlog' : ''}" style="width:${width}%"></span></span>
        <span class="t-stats">${t.published} publ., ${state}</span>
      </li>`;
    }).join('');
    lastTopicCounts = Object.fromEntries(s.topics.map((t) => [t.name, t.published]));
  }

  // clienți / cozi
  const subs = s.subscribers.filter((x) => x.role !== 'monitor');
  $('#subs').innerHTML = subs.length ? subs.map((x) => `
    <tr class="${x.online ? 'on' : 'off'}">
      <td><span class="who">${esc(x.name)}</span><span class="addr">${esc(x.role)}${x.online ? ', ' + esc(x.addr) : ', offline (durabil)'}</span></td>
      <td>${x.patterns.length ? x.patterns.map((p) => `<code>${esc(p)}</code>`).join(' ') : '<span class="addr">nu primește</span>'}</td>
      <td>${fmtBadge(x.format)}</td>
      <td class="n${x.queued ? ' hot' : ''}">${x.queued}</td>
      <td class="n${x.inflight ? ' hot' : ''}">${x.inflight}</td>
    </tr>`).join('') : '<tr><td colspan="5" class="empty">Niciun client conectat.</td></tr>';

  $('#dead').innerHTML = s.dead_letters.length ? s.dead_letters.map((d) => `
    <li><span class="who">${esc(d.from)}</span> ${esc(d.reason)}${d.raw || d.payload ? `<code>${esc(d.raw || d.payload)}</code>` : ''}</li>`).join('')
    : '<li class="empty">Nicio problemă până acum.</li>';

  $('#log').innerHTML = s.events.map((e) => `<li class="k-${esc(e.kind)}"><time>${time(e.t)}</time><span>${esc(e.text)}</span></li>`).join('');
}

function setBroker(online, addr) {
  const el = $('#brokerStatus');
  el.className = 'status ' + (online ? 'on' : 'off');
  $('#brokerText').textContent = online ? `Broker conectat${addr ? ' la ' + addr : ''}` : 'Brokerul nu răspunde. Pornește broker.py și aștept reconectarea.';
}

// =============================================================== publicare
$('#publishForm').addEventListener('submit', async (ev) => {
  ev.preventDefault();
  const f = new FormData(ev.target);
  const btn = $('button[type=submit]', ev.target);
  btn.disabled = true;
  try {
    const res = await api('POST', '/api/publish', {
      sender: f.get('sender'), topic: f.get('topic'), format: f.get('format'),
      payload: f.get('payload'), count: Number(f.get('count')),
    });
    const last = res.results[res.results.length - 1];
    const okCount = res.results.filter((r) => r.ok).length;
    $('#lastWire').hidden = false;
    $('#lastWireMeta').innerHTML = `${esc(res.sender)} a trimis ${okCount}/${res.results.length} mesaj(e) ca ${fmtBadge(f.get('format'))}`;
    $('#lastWireSent').textContent = pretty(last.wire);
    $('#lastWireReply').textContent = pretty(last.reply) || last.error;
    $('#lastWireReply').classList.toggle('err', !last.ok);
  } catch (e) {
    $('#lastWire').hidden = false;
    $('#lastWireMeta').textContent = 'Publicarea a eșuat';
    $('#lastWireSent').textContent = '';
    $('#lastWireReply').textContent = e.message;
    $('#lastWireReply').classList.add('err');
  } finally { btn.disabled = false; }
});

const PRESETS = {
  badjson: '{"type": "publish", "topic": "news.sport", "payload": "fără acoladă de final"',
  notopic: '<message><type>publish</type><payload>unde merg?</payload></message>',
  unknown: '{"type": "teleport", "topic": "news.sport"}',
  checksum: '<message><type>publish</type><topic>news.sport</topic><payload>text modificat pe drum</payload><checksum>0000000000000000000000000000000000000000000000000000000000000000</checksum></message>',
  garbage: 'salut broker, ce faci?',
};
$('#presets').addEventListener('click', (ev) => {
  const p = ev.target.dataset.p;
  if (p) $('#rawText').value = PRESETS[p];
});
$('#rawSend').addEventListener('click', async () => {
  const out = $('#rawReply');
  out.hidden = false;
  try {
    const sender = new FormData($('#publishForm')).get('sender');
    const res = await api('POST', '/api/raw', { text: $('#rawText').value, sender });
    out.textContent = res.reply ? pretty(res.reply.raw) : 'Niciun răspuns în 1,5 s.';
    out.classList.toggle('err', res.reply?.msg?.type === 'error');
  } catch (e) { out.textContent = e.message; out.classList.add('err'); }
});

// =============================================================== receiveri
const cards = new Map();

function upsertReceiver(r) {
  let card = cards.get(r.name);
  if (!card) {
    card = $('#receiverTpl').content.firstElementChild.cloneNode(true);
    card.dataset.name = r.name;
    $('.r-name', card).textContent = r.name;
    $('.r-autoack', card).addEventListener('change', (e) => api('POST', `/api/receivers/${encodeURIComponent(r.name)}/autoack`, { value: e.target.checked }));
    $('.r-disconnect', card).addEventListener('click', () => api('DELETE', `/api/receivers/${encodeURIComponent(r.name)}`, {}));
    $('.r-forget', card).addEventListener('click', () => api('DELETE', `/api/receivers/${encodeURIComponent(r.name)}`, { forget: true }));
    $('#receiverList').prepend(card);
    cards.set(r.name, card);
  }
  $('.r-meta', card).innerHTML = `${r.topics.map((t) => `<code>${esc(t)}</code>`).join(', ')}, primește ${fmtBadge(r.format)}`;
  $('.r-autoack', card).checked = r.autoAck;
  const conn = $('.conn', card);
  conn.textContent = r.online ? 'conectat' : 'reconectare…';
  conn.classList.toggle('off', !r.online);
  $('#noReceivers').hidden = cards.size > 0;
}

function addDelivery({ receiver, msg, raw, acked }) {
  const card = cards.get(receiver);
  if (!card) return;
  const inbox = $('.inbox', card);
  $('.empty', inbox)?.remove();
  const li = document.createElement('li');
  li.className = 'arrive' + (acked ? '' : ' pending') + (msg.verified ? '' : ' broken');
  const attempt = Number(msg.attempt) > 1 ? `, încercarea ${esc(msg.attempt)}` : '';
  li.innerHTML = `
    <div class="m-head">
      <span class="topic">${esc(msg.topic)}</span>
      <span>de la ${esc(msg.sender)}</span>
      <span>trimis ${fmtBadge(msg.format)}${attempt}</span>
      <span class="${msg.verified ? 'ok' : 'ko'}">${msg.verified ? 'SHA-256 ok' : 'SHA-256 greșit'}</span>
    </div>
    <p class="m-body">${esc(msg.payload)}</p>
    <div class="m-foot">
      <details><summary>Cum a sosit pe fir</summary><pre>${esc(pretty(raw))}</pre></details>
      ${acked ? `<span class="hint">${time(msg.timestamp)}</span>` : '<button type="button">Confirmă (ACK)</button>'}
    </div>`;
  const btn = $('.m-foot button', li);
  if (btn) btn.addEventListener('click', async () => {
    await api('POST', `/api/receivers/${encodeURIComponent(receiver)}/ack`, { id: msg.id });
    li.classList.remove('pending');
    btn.replaceWith(Object.assign(document.createElement('span'), { className: 'hint', textContent: 'confirmat' }));
  });
  inbox.prepend(li);
  while (inbox.children.length > 30) inbox.lastElementChild.remove();
}

$('#receiverForm').addEventListener('submit', async (ev) => {
  ev.preventDefault();
  const f = new FormData(ev.target);
  const err = $('#recvError');
  err.hidden = true;
  try {
    await api('POST', '/api/receivers', {
      name: f.get('name'), format: f.get('format'), autoAck: f.get('autoAck') === 'on',
      topics: String(f.get('topics')).split(','),
    });
    const n = ev.target.elements.name;
    n.value = n.value.replace(/(\d+)$/, (d) => Number(d) + 1);
  } catch (e) { err.textContent = e.message; err.hidden = false; }
});

// =============================================================== evenimente live
async function init() {
  const st = await api('GET', '/api/state');
  setBroker(st.brokerOnline, st.broker);
  st.receivers.forEach(upsertReceiver);

  const es = new EventSource('/api/events');
  es.addEventListener('broker', (e) => setBroker(JSON.parse(e.data).online, st.broker));
  es.addEventListener('stats', (e) => renderStats(JSON.parse(e.data)));
  es.addEventListener('receiver', (e) => upsertReceiver(JSON.parse(e.data)));
  es.addEventListener('receiver-removed', (e) => {
    const { name } = JSON.parse(e.data);
    cards.get(name)?.remove();
    cards.delete(name);
    $('#noReceivers').hidden = cards.size > 0;
  });
  es.addEventListener('deliver', (e) => addDelivery(JSON.parse(e.data)));
  es.onerror = () => setBroker(false);
  es.onopen = () => api('GET', '/api/state').then((s) => setBroker(s.brokerOnline, s.broker));
}
init();
