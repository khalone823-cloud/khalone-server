// 금빛 여울 온라인 서버 — 채팅 · 플레이어 경매장 · 랭킹 · 전시장 구경
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const DATA = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const PUB = path.join(__dirname, 'public');
const MIN = process.env.FAST ? 1000 : 60000; // 테스트용: FAST=1이면 1분=1초

let D = { players: {}, auc: {}, mail: {}, chat: [] };
try { D = Object.assign(D, JSON.parse(fs.readFileSync(DATA, 'utf8'))); } catch (e) {}
let dirty = false;
setInterval(() => { if (dirty) { dirty = false; fs.writeFile(DATA, JSON.stringify(D), () => {}); } }, 5000);

const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.json': 'application/json', '.ico': 'image/x-icon' };
const server = http.createServer((req, res) => {
  let p = decodeURIComponent((req.url || '/').split('?')[0]);
  if (p === '/health') { res.writeHead(200); res.end('ok'); return; }
  if (p === '/' || p === '') p = '/index.html';
  // index.html은 public 폴더에 있으면 거기서, 없으면 server.js 옆에서 찾음 (파일 3개만 올려도 동작)
  let f = path.join(PUB, path.normalize(p));
  if (p === '/index.html' && !fs.existsSync(f)) f = path.join(__dirname, 'index.html');
  else if (!f.startsWith(PUB)) { res.writeHead(403); res.end(); return; }
  fs.readFile(f, (e, b) => {
    if (e) { res.writeHead(404); res.end('not found'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream' });
    res.end(b);
  });
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 2 * 1024 * 1024 });
const conns = new Map(); // ws -> 플레이어 id
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const bc = o => { const s = JSON.stringify(o); wss.clients.forEach(c => { if (c.readyState === 1) c.send(s); }); };
const socketsOf = id => [...conns].filter(([, i]) => i === id).map(([w]) => w);
const online = () => new Set(conns.values()).size;
const clean = (t, n) => String(t == null ? '' : t).replace(/[\u0000-\u001f]/g, '').slice(0, n);
const specName = s => clean((s && s.nm) || '표본', 40);

function rank() {
  return Object.entries(D.players)
    .map(([id, p]) => ({ id, name: p.name, ...(p.profile || {}), disp: (p.disp || []).length }))
    .sort((a, b) => (b.total || 0) - (a.total || 0)).slice(0, 50);
}
function players() {
  return Object.entries(D.players).filter(([, p]) => (p.disp || []).length)
    .map(([id, p]) => ({ id, name: p.name, disp: p.disp.length, total: (p.profile || {}).total || 0 }))
    .sort((a, b) => b.total - a.total).slice(0, 50);
}
const aucList = () => Object.values(D.auc).sort((a, b) => a.end - b.end);
function mail(id, m) {
  m.mid = Math.random().toString(36).slice(2) + Date.now().toString(36);
  (D.mail[id] = D.mail[id] || []).push(m); dirty = true;
  socketsOf(id).forEach(w => send(w, { t: 'mail', mail: [m] }));
}
function sysChat(text) {
  const m = { t: 'chat', sys: 1, text, ts: Date.now() };
  D.chat.push(m); if (D.chat.length > 60) D.chat.shift(); dirty = true; bc(m);
}
let rankT = 0;
function pushRank() { clearTimeout(rankT); rankT = setTimeout(() => bc({ t: 'rank', rank: rank(), players: players() }), 1000); }

function resolve(a) {
  delete D.auc[a.lid]; dirty = true;
  if (a.top) {
    const pay = Math.floor(a.top.amt * 0.95); // 수수료 5%
    mail(a.sid, { type: 'sold', lid: a.lid, amt: pay, name: a.name, buyer: a.top.name });
    mail(a.top.id, { type: 'won', lid: a.lid, spec: a.spec, amt: a.top.amt });
    sysChat(`${a.sname}님의 ${a.name}이(가) ${a.top.name}님에게 ₩${a.top.amt.toLocaleString()}에 낙찰됐어요`);
  } else mail(a.sid, { type: 'unsold', lid: a.lid });
  bc({ t: 'auc', auc: aucList() });
}
setInterval(() => { const now = Date.now(); for (const a of Object.values(D.auc)) if (a.end <= now) resolve(a); }, 1000);

wss.on('connection', ws => {
  ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    const id = conns.get(ws);
    if (m.t === 'hello') {
      const pid = clean(m.id, 40); if (!pid) return;
      conns.set(ws, pid);
      const p = D.players[pid] = D.players[pid] || {};
      p.name = clean(m.name, 12) || '광부'; p.profile = m.profile || {}; p.disp = Array.isArray(m.disp) ? m.disp.slice(0, 40) : []; p.seen = Date.now();
      const pending = lid => (D.mail[pid] || []).some(x => x.lid === lid);
      // 서버가 재시작돼 사라진 내 경매를 복구
      (m.listings || []).forEach(L => {
        if (!L || !L.lid || D.auc[L.lid] || String(L.lid).indexOf(pid) !== 0 || pending(L.lid)) return;
        D.auc[L.lid] = { lid: L.lid, sid: pid, sname: p.name, spec: L.spec, name: specName(L.spec), start: Math.max(1, L.start | 0), end: Math.max(+L.end || 0, Date.now() + MIN), top: null, n: 0 };
      });
      // 서버에 기록이 없는 입찰금은 돌려줌
      (m.bids || []).forEach(b => {
        const a = D.auc[b.lid]; if (pending(b.lid)) return;
        if (!a || !a.top || a.top.id !== pid) mail(pid, { type: 'refund', lid: b.lid, amt: b.amt, why: 'sync' });
      });
      dirty = true;
      send(ws, { t: 'welcome', chat: D.chat.slice(-40), rank: rank(), players: players(), auc: aucList(), online: online() });
      if ((D.mail[pid] || []).length) send(ws, { t: 'mail', mail: D.mail[pid] });
      bc({ t: 'online', n: online() }); bc({ t: 'auc', auc: aucList() }); pushRank();
      return;
    }
    if (!id) return;
    const p = D.players[id];
    if (m.t === 'chat') {
      const text = clean(m.text, 200).trim(); if (!text) return;
      const now = Date.now(); if (p.lastChat && now - p.lastChat < 700) return; p.lastChat = now;
      const c = { t: 'chat', name: p.name, text, ts: now };
      D.chat.push(c); if (D.chat.length > 60) D.chat.shift(); dirty = true; bc(c);
    } else if (m.t === 'profile') {
      p.profile = m.profile || {}; p.disp = Array.isArray(m.disp) ? m.disp.slice(0, 40) : []; dirty = true; pushRank();
    } else if (m.t === 'rank?') {
      send(ws, { t: 'rank', rank: rank(), players: players() });
    } else if (m.t === 'disp?') {
      const q = D.players[clean(m.pid, 40)];
      if (q) send(ws, { t: 'disp', pid: m.pid, name: q.name, specs: q.disp || [] });
    } else if (m.t === 'list') {
      const lid = clean(m.lid, 80);
      if (!lid || D.auc[lid] || lid.indexOf(id) !== 0 || !m.spec) return;
      if (Object.values(D.auc).filter(a => a.sid === id).length >= 5) { send(ws, { t: 'biderr', msg: '한 사람당 5건까지 출품할 수 있어요' }); mail(id, { type: 'unsold', lid }); return; }
      const dur = [5, 15, 60, 360].includes(+m.dur) ? +m.dur : 15, start = Math.max(1, Math.floor(+m.start || 1));
      D.auc[lid] = { lid, sid: id, sname: p.name, spec: m.spec, name: specName(m.spec), start, end: Date.now() + dur * MIN, top: null, n: 0 };
      dirty = true; send(ws, { t: 'listed', lid, start }); bc({ t: 'auc', auc: aucList() });
      sysChat(`${p.name}님이 ${specName(m.spec)}을(를) 경매에 올렸어요`);
    } else if (m.t === 'bid') {
      const a = D.auc[clean(m.lid, 80)], amt = Math.floor(+m.amt || 0);
      if (!a) { send(ws, { t: 'biderr', msg: '이미 끝난 경매예요' }); return; }
      if (a.sid === id) { send(ws, { t: 'biderr', msg: '내 출품에는 입찰할 수 없어요' }); return; }
      if (a.top && a.top.id === id) { send(ws, { t: 'biderr', msg: '이미 최고가예요' }); return; }
      const min = a.top ? Math.ceil(a.top.amt * 1.05) : a.start;
      if (amt < min) { send(ws, { t: 'biderr', msg: `최소 ₩${min.toLocaleString()} 이상 입찰해야 해요` }); return; }
      const prev = a.top; a.top = { id, name: p.name, amt }; a.n++;
      if (a.end - Date.now() < MIN / 2) a.end = Date.now() + MIN / 2; // 마감 직전 입찰은 30초 연장
      dirty = true; send(ws, { t: 'bidok', lid: a.lid, amt });
      if (prev) mail(prev.id, { type: 'refund', lid: a.lid, amt: prev.amt });
      bc({ t: 'auc', auc: aucList() });
    } else if (m.t === 'ack') {
      const L = D.mail[id]; if (L) { D.mail[id] = L.filter(x => x.mid !== m.mid); dirty = true; }
    }
  });
  ws.on('close', () => { conns.delete(ws); bc({ t: 'online', n: online() }); });
});
setInterval(() => { wss.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping(); }); }, 30000);

server.listen(PORT, () => console.log('금빛 여울 서버 실행 중 : ' + PORT));
