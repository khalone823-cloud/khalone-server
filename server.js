// 광물 채집 플랫폼 서버 — 탐석(함께 탐석·박물관·거래소) + 금빛 여울(채팅·플레이어 경매장·랭킹·전시장)
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const DATA = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const PUB = path.join(__dirname, 'public');
const MIN = process.env.FAST ? 1000 : 60000; // 테스트용: FAST=1이면 1분=1초

let D = { players: {}, auc: {}, mail: {}, chat: [], ts: { museums: {}, market: {}, payouts: {}, returns: {} } };
try { D = Object.assign(D, JSON.parse(fs.readFileSync(DATA, 'utf8'))); } catch (e) {}
D.ts = Object.assign({ museums: {}, market: {}, payouts: {}, returns: {} }, D.ts || {});
let dirty = false;
setInterval(() => { if (dirty) { dirty = false; fs.writeFile(DATA, JSON.stringify(D), () => {}); } }, 5000);

const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.json': 'application/json', '.ico': 'image/x-icon' };
// 정적 파일: 허용된 파일만 (public 폴더가 있으면 거기서, 없으면 server.js 옆에서)
const FILES = { '/': 'index.html', '/index.html': 'index.html', '/tamseok.html': 'tamseok.html', '/geumbit.html': 'geumbit.html' };
const server = http.createServer((req, res) => {
  const p = decodeURIComponent((req.url || '/').split('?')[0]);
  if (p === '/health') { res.writeHead(200); res.end('ok'); return; }
  const name = FILES[p];
  if (!name) { res.writeHead(404); res.end('not found'); return; }
  let f = path.join(PUB, name);
  if (!fs.existsSync(f)) f = path.join(__dirname, name);
  fs.readFile(f, (e, b) => {
    if (e) { res.writeHead(404); res.end('not found'); return; }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(b);
  });
});

// 금빛 여울: /ws-gb , 탐석: /ws
const wss = new WebSocketServer({ noServer: true, maxPayload: 4 * 1024 * 1024 });
const tss = new WebSocketServer({ noServer: true, maxPayload: 4 * 1024 * 1024 });
server.on('upgrade', (req, sock, head) => {
  const p = (req.url || '').split('?')[0];
  if (p === '/ws-gb') wss.handleUpgrade(req, sock, head, ws => wss.emit('connection', ws, req));
  else if (p === '/ws') tss.handleUpgrade(req, sock, head, ws => tss.emit('connection', ws, req));
  else sock.destroy();
});
const conns = new Map(); // ws -> 플레이어 id
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const bc = o => { const s = JSON.stringify(o); wss.clients.forEach(c => { if (c.readyState === 1) c.send(s); }); };
const socketsOf = id => [...conns].filter(([, i]) => i === id).map(([w]) => w);
const online = () => new Set(conns.values()).size;
// 공유 지역 '만남의 여울': 접속 중인 플레이어 위치 (저장하지 않음)
const PLAZA = new Map();
const plazaSend = (o, except) => { const s = JSON.stringify(o); for (const [w, pid] of conns) if (pid !== except && PLAZA.has(pid) && w.readyState === 1) w.send(s); };
const plazaLeave = id => { if (PLAZA.delete(id)) { plazaSend({ t: 'pgone', id }, id); bc({ t: 'online', n: online(), pz: PLAZA.size }); } };
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
      bc({ t: 'online', n: online(), pz: PLAZA.size }); bc({ t: 'auc', auc: aucList() }); pushRank();
      return;
    }
    if (!id) return;
    const p = D.players[id];
    if (m.t === 'chat') {
      const text = clean(m.text, 200).trim(); if (!text) return;
      const now = Date.now(); if (p.lastChat && now - p.lastChat < 700) return; p.lastChat = now;
      const c = { t: 'chat', id, name: p.name, text, ts: now };
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
    } else if (m.t === 'zone') {
      if (m.z === 'plaza') {
        if (!PLAZA.has(id)) {
          PLAZA.set(id, { name: p.name, x: +m.x || 0, z: +m.z2 || 0, f: 0, st: 0 });
          send(ws, { t: 'peers', list: [...PLAZA].filter(([k]) => k !== id).map(([k, v]) => ({ id: k, name: v.name, x: v.x, z: v.z, f: v.f })) });
          plazaSend({ t: 'ppos', id, name: p.name, x: +m.x || 0, z: +m.z2 || 0, f: 0 }, id);
          bc({ t: 'online', n: online(), pz: PLAZA.size });
        }
      } else plazaLeave(id);
    } else if (m.t === 'pos') {
      const q = PLAZA.get(id); if (!q) return;
      const now = Date.now(); if (now - q.st < 70) return; q.st = now;
      q.x = Math.max(-200, Math.min(200, +m.x || 0)); q.z = Math.max(-200, Math.min(200, +m.z || 0)); q.f = +m.f || 0; q.name = p.name;
      plazaSend({ t: 'ppos', id, name: p.name, x: q.x, z: q.z, f: q.f }, id);
    } else if (m.t === 'aend' || m.t === 'acancel') {
      // 판매자가 즉시 마감(최고가에 낙찰) 또는 취소(입찰금 환불, 표본 반환)
      const a = D.auc[clean(m.lid, 80)];
      if (!a) { send(ws, { t: 'biderr', msg: '이미 끝난 경매예요' }); return; }
      if (a.sid !== id) { send(ws, { t: 'biderr', msg: '내 출품만 마감하거나 취소할 수 있어요' }); return; }
      if (m.t === 'aend') { resolve(a); send(ws, { t: 'aended', lid: a.lid, sold: !!a.top }); }
      else {
        delete D.auc[a.lid]; dirty = true;
        if (a.top) mail(a.top.id, { type: 'refund', lid: a.lid, amt: a.top.amt, why: 'cancel' });
        mail(a.sid, { type: 'unsold', lid: a.lid, why: 'cancel' });
        sysChat(`${a.sname}님이 ${a.name} 경매를 취소했어요`);
        send(ws, { t: 'acanceled', lid: a.lid }); bc({ t: 'auc', auc: aucList() });
      }
    } else if (m.t === 'ack') {
      const L = D.mail[id]; if (L) { D.mail[id] = L.filter(x => x.mid !== m.mid); dirty = true; }
    }
  });
  ws.on('close', () => { const id = conns.get(ws); conns.delete(ws); if (id && !socketsOf(id).length) plazaLeave(id); bc({ t: 'online', n: online(), pz: PLAZA.size }); });
});
/* ============================ 탐석 ============================
   같은 시드 = 같은 방(위치 동기화, 바위 파괴 공유, 표본 선착순), 박물관 공개/방문, 표본 거래소 */
const crypto = require('crypto');
const T = D.ts;
const tsInfo = new Map(); // ws -> { id, nick, room, x, z, h }
const rooms = new Map();  // seed -> { broken:Set, taken:{}, members:Set<ws>, empty:0 }
const tsSend = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };
const tsAll = o => { const s = JSON.stringify(o); tss.clients.forEach(c => { if (c.readyState === 1 && tsInfo.has(c)) c.send(s); }); };
const tsOf = id => [...tsInfo].filter(([, i]) => i.id === id).map(([w]) => w);
function roomOf(seed) { let r = rooms.get(seed); if (!r) { r = { broken: new Set(), taken: {}, members: new Set(), empty: 0 }; rooms.set(seed, r); } return r; }
function tsRoomCast(r, o, except) { const s = JSON.stringify(o); for (const w of r.members) if (w !== except && w.readyState === 1) w.send(s); }
function tsLeave(ws) { const i = tsInfo.get(ws); if (!i || i.room == null) return; const r = rooms.get(i.room); if (r) { r.members.delete(ws); tsRoomCast(r, { t: 'bye', id: i.id }); if (!r.members.size) r.empty = Date.now(); } i.room = null; }
const marketList = () => Object.values(T.market).sort((a, b) => b.t - a.t).slice(0, 200).map(l => ({ lid: l.lid, nick: l.nick, price: l.price, seller: l.seller, spec: l.spec, title: l.title }));
setInterval(() => { const now = Date.now(); for (const [k, r] of rooms) if (!r.members.size && now - r.empty > 30 * 60000) rooms.delete(k); }, 60000);

tss.on('connection', ws => {
  ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    let i = tsInfo.get(ws);
    if (m.t === 'hello') {
      const tok = clean(m.tok, 64); if (tok.length < 8) return;
      const id = crypto.createHash('sha1').update('ts:' + tok).digest('hex').slice(0, 12);
      i = { id, nick: clean(m.nick, 16) || '탐석가', room: null, x: 0, z: 0, h: 0 };
      tsInfo.set(ws, i);
      tsSend(ws, { t: 'welcome', id, payouts: T.payouts[id] || [], returns: T.returns[id] || [] });
      delete T.payouts[id]; delete T.returns[id]; dirty = true;
      return;
    }
    if (!i) return;
    switch (m.t) {
      case 'nick': i.nick = clean(m.nick, 16) || i.nick; break;
      case 'join': {
        const seed = m.seed; if (typeof seed !== 'number' && typeof seed !== 'string') return;
        tsLeave(ws); const r = roomOf(seed); i.room = seed; r.members.add(ws);
        const peers = [...r.members].filter(w => w !== ws).map(w => { const q = tsInfo.get(w); return { id: q.id, nick: q.nick, x: q.x, z: q.z, h: q.h }; });
        tsSend(ws, { t: 'world', seed, broken: [...r.broken], taken: r.taken, peers });
        tsRoomCast(r, { t: 'peer', p: { id: i.id, nick: i.nick, x: i.x, z: i.z, h: i.h } }, ws);
        break;
      }
      case 'leave': tsLeave(ws); break;
      case 'pos': {
        if (i.room == null) return; i.x = +m.x || 0; i.z = +m.z || 0; i.h = +m.h || 0;
        const r = rooms.get(i.room); if (r) tsRoomCast(r, { t: 'pos', id: i.id, x: i.x, z: i.z, h: i.h, w: m.w ? 1 : 0, r: m.r ? 1 : 0, s: m.s ? 1 : 0 }, ws);
        break;
      }
      case 'brk': {
        if (i.room == null || m.seed !== i.room) return; const r = rooms.get(i.room); if (!r) return;
        if (r.broken.size < 5000) r.broken.add(m.i); tsRoomCast(r, { t: 'brk', i: m.i, seed: m.seed, nick: i.nick }, ws);
        break;
      }
      case 'claim': {
        if (i.room == null || m.seed !== i.room) return; const r = rooms.get(i.room); if (!r) return;
        const key = String(m.key);
        if (r.taken[key] == null) { r.taken[key] = i.nick; tsSend(ws, { t: 'claim_ok', key: m.key }); tsRoomCast(r, { t: 'taken', key: m.key, seed: m.seed, nick: i.nick }, ws); }
        else tsSend(ws, { t: 'claim_no', key: m.key, nick: r.taken[key] });
        break;
      }
      case 'museum_pub': {
        const specs = Array.isArray(m.specs) ? m.specs.slice(0, 80) : [];
        if (JSON.stringify(specs).length > 3.5e6) { tsSend(ws, { t: 'err', msg: '전시 표본이 너무 커서 공개할 수 없어요' }); return; }
        if (specs.length) T.museums[i.id] = { nick: clean(m.nick, 16) || i.nick, specs, t: Date.now() }; else delete T.museums[i.id];
        dirty = true; break;
      }
      case 'museums_get':
        tsSend(ws, { t: 'museums', list: Object.entries(T.museums).sort((a, b) => b[1].t - a[1].t).slice(0, 100).map(([id, x]) => ({ id, nick: x.nick, n: x.specs.length, top: x.specs[0] && (x.specs[0].name || null) })) });
        break;
      case 'museum_get': { const x = T.museums[clean(m.id, 20)]; tsSend(ws, { t: 'museum', nick: x ? x.nick : '탐석가', specs: x ? x.specs : [] }); break; }
      case 'market_get': tsSend(ws, { t: 'market', list: marketList(), mine: [] }); break;
      case 'list': {
        const price = Math.floor(+m.price || 0);
        if (!m.spec || typeof m.spec !== 'object' || price < 1000 || price > 1e11) { tsSend(ws, { t: 'err', msg: '가격이 올바르지 않아요' }); return; }
        if (Object.values(T.market).filter(l => l.seller === i.id).length >= 10) { tsSend(ws, { t: 'err', msg: '한 사람당 10개까지 올릴 수 있어요' }); return; }
        const lid = crypto.randomBytes(6).toString('hex');
        T.market[lid] = { lid, seller: i.id, nick: i.nick, price, spec: m.spec, title: clean(m.title, 60), t: Date.now() };
        dirty = true; tsSend(ws, { t: 'listed', sid: m.sid }); tsAll({ t: 'market', list: marketList(), mine: [] });
        break;
      }
      case 'unlist': {
        const l = T.market[clean(m.lid, 20)]; if (!l || l.seller !== i.id) return;
        delete T.market[l.lid]; dirty = true; tsSend(ws, { t: 'unlisted', spec: l.spec }); tsAll({ t: 'market', list: marketList(), mine: [] });
        break;
      }
      case 'buy': {
        const l = T.market[clean(m.lid, 20)];
        if (!l) { tsSend(ws, { t: 'buy_no', why: '이미 팔린 표본이에요.' }); return; }
        if (l.seller === i.id) { tsSend(ws, { t: 'buy_no', why: '내가 올린 표본이에요.' }); return; }
        delete T.market[l.lid]; dirty = true;
        tsSend(ws, { t: 'bought', spec: l.spec, price: l.price });
        const sellers = tsOf(l.seller);
        if (sellers.length) sellers.forEach(w => tsSend(w, { t: 'sold', price: l.price, title: l.title }));
        else (T.payouts[l.seller] = T.payouts[l.seller] || []).push({ price: l.price, title: l.title });
        tsAll({ t: 'market', list: marketList(), mine: [] });
        break;
      }
    }
  });
  ws.on('close', () => { tsLeave(ws); tsInfo.delete(ws); });
});

setInterval(() => { for (const W of [wss, tss]) W.clients.forEach(ws => { if (!ws.isAlive) return ws.terminate(); ws.isAlive = false; ws.ping(); }); }, 30000);

server.listen(PORT, () => console.log('광물 채집 플랫폼 서버 실행 중 : ' + PORT));
