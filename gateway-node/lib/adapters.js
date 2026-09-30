'use strict';
/**
 * Șablonul Adapter (Node.js): aceeași interfață pentru JSON și XML.
 * Modelul intern = obiect plat cu valori string.
 *
 *   interface MessageAdapter { name; serialize(msg): Buffer; deserialize(buf): object }
 */

class FormatError extends Error {}

class JsonAdapter {
  get name() { return 'json'; }

  serialize(msg) {
    return Buffer.from(JSON.stringify(clean(msg)), 'utf8');
  }

  deserialize(buf) {
    let obj;
    try { obj = JSON.parse(buf.toString('utf8')); }
    catch (e) { throw new FormatError(`JSON invalid: ${e.message}`); }
    if (!obj || typeof obj !== 'object' || Array.isArray(obj)) throw new FormatError('rădăcina JSON trebuie să fie obiect');
    for (const [k, v] of Object.entries(obj)) {
      if (typeof v !== 'string') throw new FormatError(`câmpul '${k}' trebuie să fie string`);
    }
    return obj;
  }
}

const ESC = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&apos;' };
const UNESC = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'" };
const escapeXml = (s) => s.replace(/[&<>"']/g, (c) => ESC[c]);
const unescapeXml = (s) => s.replace(/&(#x[0-9a-fA-F]+|#[0-9]+|amp|lt|gt|quot|apos);/g, (_, e) => {
  if (e[0] === '#') return String.fromCodePoint(e[1] === 'x' ? parseInt(e.slice(2), 16) : parseInt(e.slice(1), 10));
  return UNESC[e];
});

class XmlAdapter {
  get name() { return 'xml'; }

  serialize(msg) {
    const body = Object.entries(clean(msg)).map(([k, v]) => `<${k}>${escapeXml(v)}</${k}>`).join('');
    return Buffer.from(`<message>${body}</message>`, 'utf8');
  }

  // Parser minimal pentru mesaje plate: <message><camp>text</camp>...</message>
  deserialize(buf) {
    let s = buf.toString('utf8').trim().replace(/^<\?xml[^>]*\?>\s*/, '');
    if (/<!DOCTYPE|<!ENTITY/i.test(s)) throw new FormatError('XML cu DOCTYPE/ENTITY nu este permis');
    const root = s.match(/^<message\s*>([\s\S]*)<\/message\s*>$/);
    if (!root) throw new FormatError('XML invalid: se așteaptă <message>...</message>');
    let inner = root[1];
    const out = {};
    const re = /\s*(?:<([A-Za-z_][\w.-]*)\s*\/>|<([A-Za-z_][\w.-]*)\s*>([^<]*)<\/\2\s*>|<([A-Za-z_][\w.-]*)\s*><!\[CDATA\[([\s\S]*?)\]\]><\/\4\s*>)/y;
    let m;
    let pos = 0;
    while ((m = re.exec(inner))) {
      if (m[1]) out[m[1]] = '';
      else if (m[2]) out[m[2]] = unescapeXml(m[3]);
      else out[m[4]] = m[5];
      pos = re.lastIndex;
    }
    if (inner.slice(pos).trim() !== '') throw new FormatError('XML invalid: structură neașteptată în <message>');
    return out;
  }
}

function clean(msg) {
  const o = {};
  for (const [k, v] of Object.entries(msg)) if (v !== undefined && v !== null) o[k] = String(v);
  return o;
}

const registry = { json: new JsonAdapter(), xml: new XmlAdapter() };

const AdapterFactory = {
  get(name) {
    const a = registry[String(name).toLowerCase()];
    if (!a) throw new FormatError(`format necunoscut: ${name}`);
    return a;
  },
  detect(buf) {
    const c = buf.toString('utf8').replace(/^\uFEFF/, '').trimStart()[0];
    if (c === '{') return registry.json;
    if (c === '<') return registry.xml;
    throw new FormatError('format nedetectabil');
  },
  names: () => Object.keys(registry),
};

module.exports = { AdapterFactory, FormatError, JsonAdapter, XmlAdapter };
