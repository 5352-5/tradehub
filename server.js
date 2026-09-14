// ============================================================
//  TradeHub — single-file paper trading platform
//  Everything: DB, auth, admin, engine, WS feed, HTML pages.
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

// ---------- CONFIG ----------
const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || crypto.randomBytes(48).toString('hex');
const JWT_EXPIRES = '7d';

if (!process.env.JWT_SECRET) {
  console.log('⚠️  JWT_SECRET not set. Generated a random one for this session.');
  console.log('   Set JWT_SECRET in Railway env vars so logins survive restarts.');
}

// ---------- DATABASE ----------
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
    usdt_balance REAL DEFAULT 100000.0,
    btc_balance REAL DEFAULT 1.0,
    created_at INTEGER DEFAULT (strftime('%s','now'))
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
    created_at INTEGER DEFAULT (strftime('%s','now')),
    FOREIGN KEY (user_id) REFERENCES users(id)
  );
  CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    buy_order_id INTEGER NOT NULL,
    sell_order_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    price REAL NOT NULL,
    amount REAL NOT NULL,
    created_at INTEGER DEFAULT (strftime('%s','now'))
  );
  CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    action TEXT NOT NULL,
    details TEXT,
    created_at INTEGER DEFAULT (strftime('%s','now'))
  );
`);

const audit = (userId, action, details) =>
  db.prepare('INSERT INTO audit_log (user_id, action, details) VALUES (?,?,?)')
    .run(userId, action, details ? JSON.stringify(details) : null);

// ---------- AUTH HELPERS ----------
const hashPassword = (pw) => bcrypt.hashSync(pw, 12);
const verifyPassword = (pw, hash) => bcrypt.compareSync(pw, hash);
const signToken = (user) => jwt.sign(
  { uid: user.id, username: user.username, isAdmin: !!user.is_admin },
  JWT_SECRET, { expiresIn: JWT_EXPIRES }
);

function authRequired(req, res, next) {
  const token = req.cookies?.token || (req.headers.authorization || '').replace('Bearer ', '');
  if (!token) return res.status(401).json({ error: 'Not authenticated' });
  try {
    const payload = jwt.verify(token, JWT_SECRET);
    const user = db.prepare(
      'SELECT id, email, username, is_admin, usdt_balance, btc_balance FROM users WHERE id=?'
    ).get(payload.uid);
    if (!user) return res.status(401).json({ error: 'User not found' });
    req.user = user;
    next();
  } catch {
    return res.status(401).json({ error: 'Invalid token' });
  }
}

const adminRequired = (req, res, next) =>
  req.user?.is_admin ? next() : res.status(403).json({ error: 'Admin access required' });

// ---------- ORDER MATCHING ENGINE ----------
function matchOrders(symbol) {
  const buys = db.prepare(
    `SELECT * FROM orders WHERE symbol=? AND side='buy' AND status IN ('open','partial')
     ORDER BY price DESC, created_at ASC`
  ).all(symbol);
  const sells = db.prepare(
    `SELECT * FROM orders WHERE symbol=? AND side='sell' AND status IN ('open','partial')
     ORDER BY price ASC, created_at ASC`
  ).all(symbol);

  for (const buy of buys) {
    for (const sell of sells) {
      if (buy.status === 'filled' || sell.status === 'filled') continue;
      if (buy.price < sell.price) break;

      const remainingBuy = buy.amount - buy.filled;
      const remainingSell = sell.amount - sell.filled;
      const fill = Math.min(remainingBuy, remainingSell);
      if (fill <= 0) continue;

      const execPrice = sell.created_at <= buy.created_at ? sell.price : buy.price;

      const newBuyFilled = buy.filled + fill;
      const newSellFilled = sell.filled + fill;
      const buyStatus = newBuyFilled >= buy.amount - 1e-9 ? 'filled' : 'partial';
      const sellStatus = newSellFilled >= sell.amount - 1e-9 ? 'filled' : 'partial';

      db.prepare('UPDATE orders SET filled=?, status=? WHERE id=?').run(newBuyFilled, buyStatus, buy.id);
      db.prepare('UPDATE orders SET filled=?, status=? WHERE id=?').run(newSellFilled, sellStatus, sell.id);

      db.prepare(
        'INSERT INTO trades (buy_order_id, sell_order_id, symbol, price, amount) VALUES (?,?,?,?,?)'
      ).run(buy.id, sell.id, symbol, execPrice, fill);

      // Buyer already had price*fill reserved. Refund difference if exec < buy price.
      if (buy.price > execPrice) {
        db.prepare('UPDATE users SET usdt_balance = usdt_balance + ? WHERE id=?')
          .run((buy.price - execPrice) * fill, buy.user_id);
      }
      db.prepare('UPDATE users SET btc_balance = btc_balance + ? WHERE id=?')
        .run(fill, buy.user_id);
      db.prepare('UPDATE users SET usdt_balance = usdt_balance + ? WHERE id=?')
        .run(execPrice * fill, sell.user_id);

      audit(buy.user_id, 'trade_fill', { orderId: buy.id, price: execPrice, amount: fill });
      audit(sell.user_id, 'trade_fill', { orderId: sell.id, price: execPrice, amount: fill });

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

// ---------- AUTH ROUTES ----------
app.post('/api/auth/register', (req, res) => {
  const { email, username, password } = req.body || {};
  if (!email || !username || !password) return res.status(400).json({ error: 'Missing fields' });
  if (password.length < 8) return res.status(400).json({ error: 'Password must be 8+ chars' });

  const userCount = db.prepare('SELECT COUNT(*) as c FROM users').get().c;
  const isFirstUser = userCount === 0 ? 1 : 0;

  try {
    const info = db.prepare(
      'INSERT INTO users (email, username, password_hash, is_admin) VALUES (?,?,?,?)'
    ).run(email.toLowerCase(), username, hashPassword(password), isFirstUser);

    const user = db.prepare('SELECT * FROM users WHERE id=?').get(info.lastInsertRowid);
    audit(user.id, 'register', { username, becameAdmin: !!isFirstUser });

    const token = signToken(user);
    res.cookie('token', token, {
      httpOnly: true, sameSite: 'lax',
      secure: process.env.NODE_ENV === 'production',
      maxAge: 7 * 24 * 3600 * 1000
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
    return res.status(401).json({ error: 'Invalid credentials' });
  }
  audit(user.id, 'login', {});
  const token = signToken(user);
  res.cookie('token', token, {
    httpOnly: true, sameSite: 'lax',
    secure: process.env.NODE_ENV === 'production',
    maxAge: 7 * 24 * 3600 * 1000
  });
  res.json({ ok: true, user: { id: user.id, username: user.username, isAdmin: !!user.is_admin } });
});

app.post('/api/auth/logout', (req, res) => {
  res.clearCookie('token');
  res.json({ ok: true });
});

app.get('/api/auth/me', authRequired, (req, res) => res.json({ user: req.user }));

// ---------- TRADE ROUTES ----------
app.get('/api/trade/balance', authRequired, (req, res) => {
  const u = db.prepare('SELECT usdt_balance, btc_balance FROM users WHERE id=?').get(req.user.id);
  res.json(u);
});

app.get('/api/trade/orders', authRequired, (req, res) => {
  const rows = db.prepare('SELECT * FROM orders WHERE user_id=? ORDER BY id DESC LIMIT 100').all(req.user.id);
  res.json({ rows });
});

app.post('/api/trade/orders', authRequired, (req, res) => {
  const { symbol, side, price, amount } = req.body || {};
  if (!symbol || !side || !price || !amount) return res.status(400).json({ error: 'Missing fields' });
  if (!['buy', 'sell'].includes(side)) return res.status(400).json({ error: 'Invalid side' });

  const p = Number(price), a = Number(amount);
  if (!(p > 0) || !(a > 0)) return res.status(400).json({ error: 'Price and amount must be positive' });

  const user = db.prepare('SELECT * FROM users WHERE id=?').get(req.user.id);
  const cost = p * a;

  if (side === 'buy') {
    if (user.usdt_balance < cost) return res.status(400).json({ error: 'Insufficient USDT' });
    db.prepare('UPDATE users SET usdt_balance = usdt_balance - ? WHERE id=?').run(cost, user.id);
  } else {
    if (user.btc_balance < a) return res.status(400).json({ error: 'Insufficient BTC' });
    db.prepare('UPDATE users SET btc_balance = btc_balance - ? WHERE id=?').run(a, user.id);
  }

  const info = db.prepare(
    `INSERT INTO orders (user_id, symbol, side, type, price, amount, status)
     VALUES (?,?,?,?,?,?, 'open')`
  ).run(user.id, symbol, side, 'limit', p, a);

  audit(user.id, 'order_placed', { id: info.lastInsertRowid, symbol, side, price: p, amount: a });
  matchOrders(symbol);
  res.json({ ok: true, orderId: info.lastInsertRowid });
});

app.delete('/api/trade/orders/:id', authRequired, (req, res) => {
  const order = db.prepare('SELECT * FROM orders WHERE id=? AND user_id=?')
    .get(req.params.id, req.user.id);
  if (!order) return res.status(404).json({ error: 'Order not found' });
  if (!['open', 'partial'].includes(order.status)) return res.status(400).json({ error: 'Order not cancellable' });

  const remaining = order.amount - order.filled;
  if (order.side === 'buy') {
    db.prepare('UPDATE users SET usdt_balance = usdt_balance + ? WHERE id=?')
      .run(order.price * remaining, req.user.id);
  } else {
    db.prepare('UPDATE users SET btc_balance = btc_balance + ? WHERE id=?')
      .run(remaining, req.user.id);
  }
  db.prepare("UPDATE orders SET status='cancelled' WHERE id=?").run(order.id);
  audit(req.user.id, 'order_cancelled', { id: order.id });
  res.json({ ok: true });
});

// ---------- ADMIN ROUTES ----------
app.get('/api/admin/users', authRequired, adminRequired, (req, res) => {
  const users = db.prepare(
    'SELECT id, email, username, is_admin, usdt_balance, btc_balance, created_at FROM users ORDER BY id ASC'
  ).all();
  res.json({ users });
});

app.post('/api/admin/users/:id/promote', authRequired, adminRequired, (req, res) => {
  db.prepare('UPDATE users SET is_admin=1 WHERE id=?').run(req.params.id);
  audit(req.user.id, 'promote_user', { targetId: req.params.id });
  res.json({ ok: true });
});

app.post('/api/admin/users/:id/credit', authRequired, adminRequired, (req, res) => {
  const usdt = Number(req.body?.usdt) || 0;
  const btc = Number(req.body?.btc) || 0;
  db.prepare('UPDATE users SET usdt_balance = usdt_balance + ?, btc_balance = btc_balance + ? WHERE id=?')
    .run(usdt, btc, req.params.id);
  audit(req.user.id, 'credit_user', { targetId: req.params.id, usdt, btc });
  res.json({ ok: true });
});

app.get('/api/admin/audit', authRequired, adminRequired, (req, res) => {
  const rows = db.prepare('SELECT * FROM audit_log ORDER BY id DESC LIMIT 200').all();
  res.json({ rows });
});

app.get('/api/admin/orders', authRequired, adminRequired, (req, res) => {
  const rows = db.prepare(
    `SELECT o.*, u.username FROM orders o JOIN users u ON u.id = o.user_id
     ORDER BY o.id DESC LIMIT 200`
  ).all();
  res.json({ rows });
});

// ---------- INLINE HTML ----------
const LOGIN_HTML = `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TradeHub · Login</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0b0e11;color:#eaecef;font-family:system-ui,-apple-system,sans-serif;font-size:14px;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:20px}
.card{background:#181a20;padding:32px;border-radius:12px;width:100%;max-width:380px;box-shadow:0 20px 60px rgba(0,0,0,.6)}
h1{color:#f0b90b;margin-bottom:6px;font-size:22px}
.muted{color:#848e9c;font-size:12px;margin-bottom:20px}
.tabs{display:flex;gap:8px;margin:20px 0}
.tab{flex:1;background:transparent;border:1px solid #2b3139;color:#848e9c;padding:10px;border-radius:6px;cursor:pointer;font-size:13px;font-family:inherit}
.tab.active{background:#2b3139;color:#eaecef}
form{display:flex;flex-direction:column;gap:10px}
input{background:#1e2329;border:1px solid #2b3139;border-radius:6px;padding:12px;color:#eaecef;font-size:14px;outline:none;font-family:inherit}
input:focus{border-color:#f0b90b}
button[type=submit]{background:#f0b90b;color:#0b0e11;border:none;border-radius:6px;padding:12px;font-weight:700;cursor:pointer;font-size:14px;font-family:inherit}
button[type=submit]:hover{background:#d4a30a}
.err{color:#f6465d;margin-top:12px;font-size:12px;text-align:center}
.hint{color:#5e6673;margin-top:16px;font-size:11px;text-align:center;line-height:1.5}
</style></head><body>
<div class="card">
  <h1>⚡ TradeHub</h1>
  <div class="muted">Paper trading terminal</div>
  <div class="tabs">
    <button class="tab active" data-tab="login" type="button">Login</button>
    <button class="tab" data-tab="register" type="button">Register</button>
  </div>
  <form id="loginForm">
    <input name="username" placeholder="Username or email" required autocomplete="username">
    <input name="password" type="password" placeholder="Password" required autocomplete="current-password">
    <button type="submit">Log In</button>
  </form>
  <form id="registerForm" style="display:none">
    <input name="email" type="email" placeholder="Email" required autocomplete="email">
    <input name="username" placeholder="Username" required autocomplete="username">
    <input name="password" type="password" placeholder="Password (8+ chars)" required minlength="8" autocomplete="new-password">
    <button type="submit">Create Account</button>
  </form>
  <div class="err" id="err"></div>
  <div class="hint">First registered user becomes admin.<br>Demo only — no real money.</div>
</div>
<script>
const tabs=document.querySelectorAll('.tab'),lf=document.getElementById('loginForm'),rf=document.getElementById('registerForm'),err=document.getElementById('err');
tabs.forEach(t=>t.onclick=()=>{tabs.forEach(x=>x.classList.remove('active'));t.classList.add('active');const isL=t.dataset.tab==='login';lf.style.display=isL?'flex':'none';rf.style.display=isL?'none':'flex';err.textContent=''});
lf.onsubmit=async e=>{e.preventDefault();const d=Object.fromEntries(new FormData(lf));const r=await fetch('/api/auth/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)});const j=await r.json();if(j.ok)location.href='/';else err.textContent=j.error};
rf.onsubmit=async e=>{e.preventDefault();const d=Object.fromEntries(new FormData(rf));const r=await fetch('/api/auth/register',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)});const j=await r.json();if(j.ok)location.href='/';else err.textContent=j.error};
fetch('/api/auth/me').then(r=>{if(r.ok)location.href='/'});
</script></body></html>`;

const APP_HTML = `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TradeHub · Terminal</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0b0e11;color:#eaecef;font-family:system-ui,-apple-system,sans-serif;font-size:14px;min-height:100vh}
.topbar{background:#181a20;padding:14px 20px;display:flex;align-items:center;gap:16px;border-bottom:1px solid #2b3139;flex-wrap:wrap}
.brand{color:#f0b90b;font-weight:700;font-size:16px}
.live{display:flex;align-items:center;gap:6px;font-family:ui-monospace,monospace;color:#0ecb81;font-size:13px}
.live .dot{width:8px;height:8px;background:#0ecb81;border-radius:50%;animation:p 1.5s infinite}
@keyframes p{50%{opacity:.3}}
.nav-right{margin-left:auto;display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.nav-right span{color:#848e9c;font-size:12px}
.nav-right a{color:#f0b90b;font-size:12px;text-decoration:none}
.nav-right button{background:#2b3139;color:#eaecef;border:none;padding:8px 14px;border-radius:6px;cursor:pointer;font-family:inherit;font-size:13px}
.grid{display:grid;grid-template-columns:1fr;gap:16px;padding:16px;max-width:1400px;margin:0 auto}
@media(min-width:900px){.grid{grid-template-columns:400px 1fr}}
.panel{background:#181a20;border:1px solid #2b3139;border-radius:10px;padding:18px}
.panel h2{font-size:14px;margin-bottom:14px;color:#f0b90b}
form{display:flex;flex-direction:column;gap:12px}
label{display:flex;flex-direction:column;gap:6px;font-size:12px;color:#848e9c}
select,input{background:#1e2329;border:1px solid #2b3139;border-radius:6px;padding:12px;color:#eaecef;font-size:14px;outline:none;font-family:inherit}
select:focus,input:focus{border-color:#f0b90b}
button.primary{background:#f0b90b;color:#0b0e11;border:none;border-radius:6px;padding:12px;font-weight:700;cursor:pointer;font-size:14px;font-family:inherit}
button.primary:hover{background:#d4a30a}
.balance{margin-top:16px;padding-top:14px;border-top:1px solid #2b3139;display:flex;gap:20px;font-size:13px;flex-wrap:wrap}
.balance b{color:#f0b90b;font-family:ui-monospace,monospace}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{text-align:left;padding:8px;border-bottom:1px solid #2b3139}
th{color:#848e9c;font-weight:500;font-size:11px;text-transform:uppercase}
td button{background:#f6465d;color:#fff;border:none;padding:5px 10px;border-radius:4px;cursor:pointer;font-size:11px;font-family:inherit}
.muted{color:#848e9c;font-size:12px}
.pill{padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600}
.pill.buy{background:rgba(14,203,129,.15);color:#0ecb81}
.pill.sell{background:rgba(246,70,93,.15);color:#f6465d}
.pill.open{background:rgba(240,185,11,.15);color:#f0b90b}
.pill.filled{background:rgba(14,203,129,.15);color:#0ecb81}
.pill.partial{background:rgba(30,108,245,.15);color:#1e6cf5}
.pill.cancelled{background:rgba(132,142,156,.15);color:#848e9c}
</style></head><body>
<header class="topbar">
  <div class="brand">⚡ TradeHub</div>
  <div class="live"><span class="dot"></span><span id="livePrice">—</span></div>
  <div class="nav-right">
    <span id="userLabel"></span>
    <a href="/admin" id="adminLink" style="display:none">Admin</a>
    <button id="logoutBtn">Logout</button>
  </div>
</header>
<main class="grid">
  <section class="panel">
    <h2>New Order</h2>
    <form id="orderForm">
      <label>Symbol
        <select name="symbol"><option>BTCUSDT</option></select>
      </label>
      <label>Side
        <select name="side"><option value="buy">Buy</option><option value="sell">Sell</option></select>
      </label>
      <label>Price (USDT)
        <input name="price" type="number" step="0.01" min="0" required id="priceInput">
      </label>
      <label>Amount (BTC)
        <input name="amount" type="number" step="0.00001" min="0" required>
      </label>
      <button type="submit" class="primary">Place Order</button>
    </form>
    <div class="balance">
      <div>USDT: <b id="usdtBal">—</b></div>
      <div>BTC: <b id="btcBal">—</b></div>
    </div>
  </section>
  <section class="panel">
    <h2>My Orders</h2>
    <div id="ordersList"><p class="muted">Loading…</p></div>
  </section>
</main>
<script>
const $=s=>document.querySelector(s);
async function init(){
  const me=await fetch('/api/auth/me');
  if(!me.ok){location.href='/login';return}
  const{user}=await me.json();
  $('#userLabel').textContent=user.username;
  if(user.isAdmin)$('#adminLink').style.display='inline';
  $('#logoutBtn').onclick=async()=>{await fetch('/api/auth/logout',{method:'POST'});location.href='/login'};
  await refreshBalances();await refreshOrders();
  try{
    const ws=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws');
    ws.onmessage=e=>{const{price}=JSON.parse(e.data);$('#livePrice').textContent='$'+price.toFixed(2);if(document.activeElement!==$('#priceInput'))$('#priceInput').value=price.toFixed(2)};
  }catch(e){console.warn('WS failed',e)}
  $('#orderForm').onsubmit=async e=>{
    e.preventDefault();
    const d=Object.fromEntries(new FormData(e.target));
    const r=await fetch('/api/trade/orders',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)});
    const j=await r.json();
    if(!j.ok)return alert(j.error);
    e.target.reset();
    await refreshBalances();await refreshOrders();
  };
}
async function refreshBalances(){
  const r=await fetch('/api/trade/balance');const b=await r.json();
  $('#usdtBal').textContent=b.usdt_balance.toFixed(2);
  $('#btcBal').textContent=b.btc_balance.toFixed(4);
}
async function refreshOrders(){
  const r=await fetch('/api/trade/orders');const{rows}=await r.json();
  $('#ordersList').innerHTML=rows.length?
    '<table><thead><tr><th>ID</th><th>Side</th><th>Price</th><th>Amount</th><th>Filled</th><th>Status</th><th></th></tr></thead><tbody>'+
    rows.map(o=>'<tr><td>'+o.id+'</td><td><span class="pill '+o.side+'">'+o.side.toUpperCase()+'</span></td><td>'+o.price+'</td><td>'+o.amount+'</td><td>'+o.filled+'</td><td><span class="pill '+o.status+'">'+o.status+'</span></td><td>'+(o.status==='open'||o.status==='partial'?'<button onclick="cancelOrder('+o.id+')">Cancel</button>':'')+'</td></tr>').join('')+'</tbody></table>'
    :'<p class="muted">No orders yet.</p>';
}
window.cancelOrder=async id=>{await fetch('/api/trade/orders/'+id,{method:'DELETE'});await refreshBalances();await refreshOrders()};
init();
</script></body></html>`;

const ADMIN_HTML = `<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>TradeHub · Admin</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0b0e11;color:#eaecef;font-family:system-ui,-apple-system,sans-serif;font-size:14px}
.topbar{background:#181a20;padding:14px 20px;display:flex;align-items:center;gap:16px;border-bottom:1px solid #2b3139}
.brand{color:#f0b90b;font-weight:700}
.nav-right{margin-left:auto}
.nav-right a{color:#f0b90b;text-decoration:none;font-size:13px}
.wrap{padding:16px;max-width:1300px;margin:0 auto;display:flex;flex-direction:column;gap:16px}
.panel{background:#181a20;border:1px solid #2b3139;border-radius:10px;padding:18px;overflow-x:auto}
.panel h2{font-size:14px;margin-bottom:14px;color:#f0b90b}
table{width:100%;border-collapse:collapse;font-size:12px;min-width:520px}
th,td{text-align:left;padding:8px;border-bottom:1px solid #2b3139;white-space:nowrap}
th{color:#848e9c;font-weight:500;font-size:11px;text-transform:uppercase}
td button{background:#1e6cf5;color:#fff;border:none;padding:4px 8px;border-radius:4px;cursor:pointer;font-size:11px;margin-right:4px;font-family:inherit}
td button.danger{background:#f6465d}
code{background:#1e2329;padding:2px 6px;border-radius:3px;font-size:11px;color:#848e9c}
.err{color:#f6465d;padding:20px;text-align:center}
</style></head><body>
<header class="topbar">
  <div class="brand">⚡ TradeHub Admin</div>
  <div class="nav-right"><a href="/">← Terminal</a></div>
</header>
<div class="wrap" id="wrap"><div class="err">Loading…</div></div>
<script>
async function load(){
  const me=await fetch('/api/auth/me');
  if(!me.ok){location.href='/login';return}
  const{user}=await me.json();
  if(!user.isAdmin){document.getElementById('wrap').innerHTML='<div class="err">Admin access required.</div>';return}

  const[u,o,a]=await Promise.all([
    fetch('/api/admin/users').then(r=>r.json()),
    fetch('/api/admin/orders').then(r=>r.json()),
    fetch('/api/admin/audit').then(r=>r.json())
  ]);

  document.getElementById('wrap').innerHTML=\`
    <div class="panel"><h2>Users (\${u.users.length})</h2>
      <table><thead><tr><th>ID</th><th>Username</th><th>Email</th><th>Admin</th><th>USDT</th><th>BTC</th><th>Actions</th></tr></thead>
      <tbody>\${u.users.map(x=>\`<tr>
        <td>\${x.id}</td><td>\${x.username}</td><td>\${x.email}</td>
        <td>\${x.is_admin?'✅':'—'}</td>
        <td>\${x.usdt_balance.toFixed(2)}</td>
        <td>\${x.btc_balance.toFixed(4)}</td>
        <td>
          \${!x.is_admin?\`<button onclick="promote(\${x.id})">Promote</button>\`:''}
          <button class="danger" onclick="credit(\${x.id})">Credit</button>
        </td>
      </tr>\`).join('')}</tbody></table>
    </div>
    <div class="panel"><h2>All Orders (\${o.rows.length})</h2>
      <table><thead><tr><th>ID</th><th>User</th><th>Symbol</th><th>Side</th><th>Price</th><th>Amount</th><th>Filled</th><th>Status</th></tr></thead>
      <tbody>\${o.rows.map(r=>\`<tr>
        <td>\${r.id}</td><td>\${r.username}</td><td>\${r.symbol}</td>
        <td>\${r.side}</td><td>\${r.price}</td><td>\${r.amount}</td>
        <td>\${r.filled}</td><td>\${r.status}</td>
      </tr>\`).join('')}</tbody></table>
    </div>
    <div class="panel"><h2>Audit Log (last \${a.rows.length})</h2>
      <table><thead><tr><th>Time</th><th>User</th><th>Action</th><th>Details</th></tr></thead>
      <tbody>\${a.rows.map(r=>\`<tr>
        <td>\${new Date(r.created_at*1000).toLocaleString()}</td>
        <td>\${r.user_id||'—'}</td><td>\${r.action}</td>
        <td><code>\${(r.details||'').slice(0,80)}</code></td>
      </tr>\`).join('')}</tbody></table>
    </div>\`;
}
window.promote=async id=>{await fetch('/api/admin/users/'+id+'/promote',{method:'POST'});load()};
window.credit=async id=>{const usdt=prompt('USDT to credit:','10000');if(!usdt)return;await fetch('/api/admin/users/'+id+'/credit',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({usdt:Number(usdt)})});load()};
load();
</script></body></html>`;

// ---------- PAGE ROUTES ----------
app.get('/', (req, res) => res.type('html').send(APP_HTML));
app.get('/login', (req, res) => res.type('html').send(LOGIN_HTML));
app.get('/login.html', (req, res) => res.type('html').send(LOGIN_HTML));
app.get('/admin', (req, res) => res.type('html').send(ADMIN_HTML));
app.get('/admin.html', (req, res) => res.type('html').send(ADMIN_HTML));

// ---------- WEBSOCKET PRICE FEED ----------
const wss = new WebSocket.Server({ server, path: '/ws' });
wss.on('connection', (ws) => {
  let upstream;
  try {
    upstream = new WebSocket('wss://stream.binance.com:9443/ws/btcusdt@trade');
    upstream.on('message', (data) => {
      try {
        const m = JSON.parse(data);
        if (ws.readyState === 1) {
          ws.send(JSON.stringify({ symbol: m.s, price: parseFloat(m.p), qty: parseFloat(m.q), time: m.T }));
        }
      } catch {}
    });
    upstream.on('error', () => { try { ws.close(); } catch {} });
  } catch {}
  ws.on('close', () => { try { upstream && upstream.close(); } catch {} });
});

// ---------- START ----------
server.listen(PORT, () => {
  console.log(`✅ TradeHub running on port ${PORT}`);
  console.log(`   Login:  /login`);
  console.log(`   Admin:  /admin (first registered user is admin)`);
});
