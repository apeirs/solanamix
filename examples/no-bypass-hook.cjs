// Preload hook: node --require ./no-bypass-hook.cjs app.mjs
//
// Records every TCP connect and DNS lookup the Node process makes (http,
// https, fetch/undici and tls all end in net.Socket#connect) and flags any
// destination that is not loopback. A client that ignores the gateway URL and
// talks to a public API directly shows up here as NON-LOOPBACK.
'use strict';
const net = require('net');
const dns = require('dns');

const seen = [];
const bad = [];
function note(kind, host, port) {
  const h = String(host ?? '');
  const loop = h === 'localhost' || h === '::1' || /^127\./.test(h) || h.startsWith('/');
  const s = `${kind} ${h}${port ? ':' + port : ''}`;
  seen.push(s);
  if (!loop) {
    bad.push(s);
    process.stderr.write(`[hook] NON-LOOPBACK ${s}\n`);
  }
}

const connect = net.Socket.prototype.connect;
net.Socket.prototype.connect = function (...args) {
  let o = args[0];
  if (Array.isArray(o)) o = o[0]; // internal normalized form
  if (o && typeof o === 'object') note('connect', o.path || o.host || 'localhost', o.port);
  else if (typeof o === 'string' && isNaN(Number(o))) note('connect', o); // unix socket path
  else note('connect', typeof args[1] === 'string' ? args[1] : 'localhost', o);
  return connect.apply(this, args);
};

for (const [mod, name] of [[dns, 'lookup'], [dns.promises, 'lookup'], [dns, 'resolve4'], [dns, 'resolve6']]) {
  const orig = mod[name];
  mod[name] = function (host, ...rest) {
    note('dns', host);
    return orig.call(this, host, ...rest);
  };
}

process.on('exit', () => {
  const uniq = [...new Set(seen)];
  process.stderr.write(`[hook] ${seen.length} connects/lookups, distinct: ${uniq.join(', ') || '(none)'}\n`);
  process.stderr.write(bad.length ? `[hook] FAIL: ${bad.length} non-loopback destination(s)\n` : '[hook] OK: every connection went to loopback (the solanamix gateway)\n');
});
