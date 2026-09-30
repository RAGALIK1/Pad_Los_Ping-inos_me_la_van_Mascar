'use strict';
/**
 * Gateway web (Node.js, fără dependențe externe).
 * Face legătura dintre interfața din browser și broker:
 *   browser --HTTP/SSE--> gateway --TCP (cadre JSON/XML)--> broker
 * Fiecare sender/receiver creat din interfață este un client TCP real al brokerului.
 *
 * Rulare: node server.js [--broker 127.0.0.1:5000] [--port 8080]
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const { BrokerClient } = require('./lib/brokerClient');

const arg = (k, d) => { const i = process.argv.indexOf(k); return i > 0 ? process.argv[i + 1] : d; };
const [BROKER_HOST, BROKER_PORT] = arg('--broker', process.env.BROKER || '127.0.0.1:5000').split(':');
const HTTP_PORT = Number(arg('--port', process.env.PORT || 8080));
const NAME_RE = /^[A-Za-z0-9_.-]{1,64}$/;

const sseClients = new Set();
const broadcast = (event, data) => {
  const line = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) res.write(line);
};

// ---------------------------------------------------------------- clienți TCP
const baseOpts = { host: BROKER_HOST, port: Number(BROKER_PORT) };
const monitor = new BrokerClient({ ...baseOpts, name: 'web-monitor', role: 'monitor' });
const senders = new Map();     // nume -> BrokerClient
const receivers = new Map();   // nume -> { client, autoAck, format }

let brokerOnline = false;
function startMonitor() {
  monitor.connect().then(() => {
    brokerOnline = true;
    broadcast('broker', { online: true });
  }).catch(() => setTimeout(startMonitor, 2000));
}
monitor.on('disconnected', () => { brokerOnline = false; broadcast('broker', { online: false }); });
monitor.on('connected', () => { brokerOnline = true; broadcast('broker', { online: true }); });
startMonitor();

setInterval(async () => {
  if (!monitor.connected) return;
  try { broadcast('stats', await monitor.stats()); } catch { /* broker ocupat/oprit */ }
}, 1000);

async function getSender(name) {
  if (senders.has(name) && senders.get(name).connected) return senders.get(name);
  const c = new BrokerClient({ ...baseOpts, name, role: 'sender' });
  c.on('error-msg', (msg, raw) => broadcast('broker-error', { client: name, error: msg.error, raw }));
  await c.connect();
  senders.set(name, c);
  return c;
}

function receiverView(name) {
  const r = receivers.get(name);
  return { name, format: r.client.adapter.name, topics: [...r.client.subscriptions], autoAck: r.autoAck, online: r.client.connected };
}

async function addReceiver({ name, format, topics, autoAck }) {
  if (!NAME_RE.test(name || '')) throw new Error('numele poate conține doar litere, cifre, „.”, „-”, „_”');
  if (receivers.has(name)) throw new Error(`receiverul „${name}” există deja`);
  const client = new BrokerClient({ ...baseOpts, name, format, role: 'receiver' });
  const r = { client, autoAck: autoAck !== false };
  receivers.set(name, r);
  client.on('deliver', (msg, raw) => {
    if (r.autoAck) client.ack(msg.id);
    broadcast('deliver', { receiver: name, msg, raw, acked: r.autoAck });
  });
  client.on('error-msg', (msg, raw) => broadcast('broker-error', { client: name, error: msg.error, raw }));
  client.on('connected', () => broadcast('receiver', receiverView(name)));
  client.on('disconnected', () => receivers.has(name) && broadcast('receiver', receiverView(name)));
  try {
    const info = await client.connect();
    for (const t of topics) await client.subscribe(t);
    broadcast('receiver', receiverView(name));
    return { ...receiverView(name), restored: info };
  } catch (e) {
    receivers.delete(name);
    client.close();
    throw e;
  }
}

// ---------------------------------------------------------------- HTTP
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml' };

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (c) => { data += c; if (data.length > 2e6) req.destroy(); });
    req.on('end', () => { try { resolve(data ? JSON.parse(data) : {}); } catch (e) { reject(e); } });
  });
}

const send = (res, code, obj) => {
  res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(obj));
};

const routes = {
  'GET /api/state': async () => ({ brokerOnline, broker: `${BROKER_HOST}:${BROKER_PORT}`, receivers: [...receivers.keys()].map(receiverView) }),

  'POST /api/publish': async (b) => {
    const sender = b.sender || 'web-sender';
    if (!NAME_RE.test(sender)) throw new Error('nume de sender invalid');
    const c = await getSender(sender);
    const count = Math.min(Math.max(Number(b.count) || 1, 1), 100);
    const results = [];
    for (let i = 0; i < count; i++) {
      const payload = count > 1 ? `${b.payload} #${i + 1}` : b.payload;
      try {
        const r = await c.publish(b.topic, payload ?? '', b.format);
        results.push({ ok: true, wire: r.wire, reply: r.raw, result: r.msg.payload, id: r.msg.ref });
      } catch (e) {
        results.push({ ok: false, wire: e.wire, reply: e.raw, error: e.message });
      }
    }
    return { sender, results };
  },

  'POST /api/raw': async (b) => {
    const c = await getSender(b.sender || 'web-sender');
    const reply = new Promise((resolve) => {
      const t = setTimeout(() => { c.off('frame', h); resolve(null); }, 1500);
      const h = (msg, raw) => { clearTimeout(t); c.off('frame', h); resolve({ msg, raw }); };
      c.on('frame', h);
    });
    c.sendRaw(String(b.text ?? ''));
    return { sent: b.text, reply: await reply };
  },

  'POST /api/receivers': async (b) => addReceiver({
    name: b.name, format: b.format || 'json', autoAck: b.autoAck,
    topics: (b.topics || []).map((t) => String(t).trim()).filter(Boolean),
  }),
};

async function handleReceiverAction(method, name, action, b) {
  const r = receivers.get(name);
  if (!r) throw Object.assign(new Error(`receiverul „${name}” nu există`), { code: 404 });
  if (method === 'DELETE' && !action) {
    if (b.forget) for (const t of [...r.client.subscriptions]) await r.client.unsubscribe(t).catch(() => {});
    receivers.delete(name);
    r.client.close();
    broadcast('receiver-removed', { name, forget: !!b.forget });
    return { removed: name };
  }
  if (method === 'POST' && action === 'ack') { r.client.ack(b.id); return { acked: b.id }; }
  if (method === 'POST' && action === 'autoack') { r.autoAck = !!b.value; broadcast('receiver', receiverView(name)); return receiverView(name); }
  if (method === 'POST' && action === 'subscribe') { await r.client.subscribe(b.topic); broadcast('receiver', receiverView(name)); return receiverView(name); }
  if (method === 'POST' && action === 'unsubscribe') { await r.client.unsubscribe(b.topic); broadcast('receiver', receiverView(name)); return receiverView(name); }
  throw Object.assign(new Error('acțiune necunoscută'), { code: 404 });
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  try {
    if (req.method === 'GET' && url.pathname === '/api/events') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache', Connection: 'keep-alive' });
      res.write(`event: broker\ndata: ${JSON.stringify({ online: brokerOnline })}\n\n`);
      sseClients.add(res);
      req.on('close', () => sseClients.delete(res));
      return;
    }
    const key = `${req.method} ${url.pathname}`;
    if (routes[key]) return send(res, 200, await routes[key](await readBody(req)));

    const m = url.pathname.match(/^\/api\/receivers\/([^/]+)(?:\/(\w+))?$/);
    if (m) return send(res, 200, await handleReceiverAction(req.method, decodeURIComponent(m[1]), m[2], await readBody(req)));

    if (req.method === 'GET') {
      const file = path.join(__dirname, 'public', url.pathname === '/' ? 'index.html' : path.normalize(url.pathname));
      if (!file.startsWith(path.join(__dirname, 'public'))) return send(res, 403, { error: 'interzis' });
      return fs.readFile(file, (err, data) => {
        if (err) return send(res, 404, { error: 'nu există' });
        res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'application/octet-stream' });
        res.end(data);
      });
    }
    send(res, 404, { error: 'nu există' });
  } catch (e) {
    send(res, e.code === 404 ? 404 : 400, { error: e.message });
  }
});

server.listen(HTTP_PORT, () => {
  console.log(`Interfața web: http://localhost:${HTTP_PORT}  (broker ${BROKER_HOST}:${BROKER_PORT})`);
});
