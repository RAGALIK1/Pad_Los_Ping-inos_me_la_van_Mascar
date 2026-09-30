'use strict';
/**
 * Client TCP pentru broker (Node.js).
 * Cadre: [u32 big-endian lungime][corp JSON sau XML].
 */
const net = require('net');
const crypto = require('crypto');
const { EventEmitter } = require('events');
const { AdapterFactory } = require('./adapters');

const MAX_FRAME = 1024 * 1024;

const sha256 = (s) => crypto.createHash('sha256').update(s, 'utf8').digest('hex');

class FrameDecoder {
  constructor(onFrame) { this.buf = Buffer.alloc(0); this.onFrame = onFrame; }
  push(chunk) {
    this.buf = Buffer.concat([this.buf, chunk]);
    while (this.buf.length >= 4) {
      const len = this.buf.readUInt32BE(0);
      if (len > MAX_FRAME) throw new Error(`cadru prea mare: ${len}`);
      if (this.buf.length < 4 + len) break;
      const body = this.buf.subarray(4, 4 + len);
      this.buf = this.buf.subarray(4 + len);
      this.onFrame(Buffer.from(body));
    }
  }
}

function encodeFrame(body) {
  const header = Buffer.alloc(4);
  header.writeUInt32BE(body.length, 0);
  return Buffer.concat([header, body]);
}

/**
 * Evenimente: 'connected', 'disconnected', 'deliver' (msg, raw), 'error-msg' (msg, raw), 'frame' (msg, raw)
 */
class BrokerClient extends EventEmitter {
  constructor({ host = '127.0.0.1', port = 5000, name, format = 'json', role = 'client', autoReconnect = true }) {
    super();
    Object.assign(this, { host, port, name, role, autoReconnect });
    this.adapter = AdapterFactory.get(format);
    this.pending = new Map();      // id -> {resolve, reject, timer}
    this.connected = false;
    this.closedByUser = false;
    this.subscriptions = new Set();
  }

  connect() {
    this.closedByUser = false;
    return new Promise((resolve, reject) => {
      const sock = net.createConnection({ host: this.host, port: this.port });
      this.sock = sock;
      sock.setNoDelay(true);
      const decoder = new FrameDecoder((raw) => this._onFrame(raw));
      sock.on('data', (chunk) => {
        try { decoder.push(chunk); } catch (e) { sock.destroy(e); }
      });
      sock.once('connect', async () => {
        try {
          const ack = await this.request({ type: 'connect', sender: this.name, format: this.adapter.name, role: this.role });
          this.connected = true;
          for (const t of this.subscriptions) await this.request({ type: 'subscribe', topic: t });
          this.emit('connected', JSON.parse(ack.msg.payload || '{}'));
          resolve(this);
        } catch (e) { reject(e); }
      });
      sock.on('error', (e) => { if (!this.connected) reject(e); });
      sock.on('close', () => {
        const was = this.connected;
        this.connected = false;
        for (const p of this.pending.values()) { clearTimeout(p.timer); p.reject(new Error('conexiune închisă')); }
        this.pending.clear();
        if (was) this.emit('disconnected');
        if (this.autoReconnect && !this.closedByUser && was) {
          setTimeout(() => this.connect().catch(() => this._retryLater()), 1000);
        }
      });
    });
  }

  _retryLater() {
    if (this.closedByUser) return;
    setTimeout(() => this.connect().catch(() => this._retryLater()), 2000);
  }

  close() {
    this.closedByUser = true;
    if (this.sock) this.sock.end();
  }

  /** Trimite un mesaj; întoarce octeții exacți de pe fir (pentru afișare). */
  send(msg, format) {
    const adapter = format ? AdapterFactory.get(format) : this.adapter;
    const body = adapter.serialize(msg);
    this.sock.write(encodeFrame(body));
    return body.toString('utf8');
  }

  sendRaw(text) {
    this.sock.write(encodeFrame(Buffer.from(text, 'utf8')));
  }

  /** Cerere cu răspuns corelat prin id/ref. */
  request(msg, format, timeoutMs = 5000) {
    const id = msg.id || crypto.randomUUID();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error('timeout fără răspuns de la broker')); }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
      let wire;
      try { wire = this.send({ ...msg, id }, format); }
      catch (e) { clearTimeout(timer); this.pending.delete(id); return reject(e); }
      this.pending.get(id).wire = wire;
    });
  }

  publish(topic, payload, format) {
    return this.request({ type: 'publish', topic, payload, checksum: sha256(payload), timestamp: new Date().toISOString() }, format);
  }

  async subscribe(topic) {
    this.subscriptions.add(topic);
    return this.request({ type: 'subscribe', topic });
  }

  async unsubscribe(topic) {
    this.subscriptions.delete(topic);
    return this.request({ type: 'unsubscribe', topic });
  }

  ack(id) { this.send({ type: 'ack', ref: id }); }

  async stats() {
    const r = await this.request({ type: 'stats' });
    return JSON.parse(r.msg.payload);
  }

  _onFrame(raw) {
    let msg;
    try { msg = AdapterFactory.detect(raw).deserialize(raw); }
    catch (e) { this.emit('error-msg', { error: `răspuns nevalid de la broker: ${e.message}` }, raw.toString()); return; }
    const text = raw.toString('utf8');
    this.emit('frame', msg, text);
    if (msg.ref && this.pending.has(msg.ref)) {
      const p = this.pending.get(msg.ref);
      this.pending.delete(msg.ref);
      clearTimeout(p.timer);
      if (msg.type === 'error') {
        const err = new Error(msg.error);
        Object.assign(err, { msg, raw: text, wire: p.wire });
        p.reject(err);
      } else p.resolve({ msg, raw: text, wire: p.wire });
      return;
    }
    if (msg.type === 'deliver') {
      msg.verified = !msg.checksum || sha256(msg.payload || '') === msg.checksum;
      this.emit('deliver', msg, text);
    } else if (msg.type === 'error') this.emit('error-msg', msg, text);
  }
}

module.exports = { BrokerClient, sha256 };
