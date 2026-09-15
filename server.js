const express = require('express');
const cookieParser = require('cookie-parser');
const http = require('http');
const WebSocket = require('ws');
const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const { authenticator } = require('otplib');

const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET || crypto.randomBytes(48).toString('hex');
const DATABASE_URL = process.env.DATABASE_URL;
const USE_PG = !!DATABASE_URL;
const IS_PROD = process.env.NODE_ENV === 'production';

// ---------- INSTRUMENTS ----------
const CATS = {
  crypto:      { label: 'Crypto',      lev: 10,  spreadPct: 0.0004 },
  forex:       { label: 'Forex',       lev: 100, spreadPct: 0.00008 },
  commodities: { label: 'Commodities', lev: 50,  spreadPct: 0.0005 },
  stocks:      { label: 'Stocks',      lev: 20,  spreadPct: 0.0006 },
  indices:     { label: 'Indices',     lev: 50,  spreadPct: 0.0003 }
};

const INSTRUMENTS = [
  ['BTCUSDT','BTC','USD',64000,'crypto'],['ETHUSDT','ETH','USD',3400,'crypto'],
  ['BNBUSDT','BNB','USD',590,'crypto'],['SOLUSDT','SOL','USD',148,'crypto'],
  ['XRPUSDT','XRP','USD',0.62,'crypto'],['ADAUSDT','ADA','USD',0.45,'crypto'],
  ['DOGEUSDT','DOGE','USD',0.16,'crypto'],['AVAXUSDT','AVAX','USD',36,'crypto'],
  ['DOTUSDT','DOT','USD',6.8,'crypto'],['LINKUSDT','LINK','USD',18,'crypto'],
  ['MATICUSDT','MATIC','USD',0.88,'crypto'],['LTCUSDT','LTC','USD',84,'crypto'],

  ['EURUSD','EUR','USD',1.0842,'forex'],['GBPUSD','GBP','USD',1.2654,'forex'],
  ['USDJPY','USD','JPY',149.32,'forex'],['USDCHF','USD','CHF',0.8842,'forex'],
  ['AUDUSD','AUD','USD',0.6584,'forex'],['NZDUSD','NZD','USD',0.6012,'forex'],
  ['USDCAD','USD','CAD',1.3628,'forex'],['EURGBP','EUR','GBP',0.8567,'forex'],
  ['EURJPY','EUR','JPY',161.92,'forex'],['GBPJPY','GBP','JPY',188.92,'forex'],

  ['XAUUSD','XAU','USD',2358.4,'commodities'],['XAGUSD','XAG','USD',27.82,'commodities'],
  ['WTIUSD','WTI','USD',78.42,'commodities'],['BRENTUSD','BRENT','USD',82.14,'commodities'],
  ['NATGASUSD','NATGAS','USD',2.14,'commodities'],['COPPERUSD','COPPER','USD',4.42,'commodities'],

  ['AAPLUSD','AAPL','USD',224.15,'stocks'],['TSLAUSD','TSLA','USD',248.5,'stocks'],
  ['NVDAUSD','NVDA','USD',128.42,'stocks'],['MSFTUSD','MSFT','USD',442.18,'stocks'],
  ['GOOGLUSD','GOOGL','USD',168.74,'stocks'],['AMZNUSD','AMZN','USD',185.32,'stocks'],
  ['METAUSD','META','USD',512.6,'stocks'],['NFLXUSD','NFLX','USD',712.4,'stocks'],

  ['SPX500','SPX','USD',5620.3,'indices'],['NAS100','NAS','USD',19840.5,'indices'],
  ['DJ30','DJ','USD',41240.8,'indices'],['FTSE100','FTSE','USD',8240.15,'indices'],
  ['DAX40','DAX','USD',18420.4,'indices'],['NIKKEI225','N225','USD',38420.6,'indices']
].map(([symbol,base,quote,start,cat]) => ({ symbol, base, quote, start, cat }));

const INST_MAP = Object.fromEntries(INSTRUMENTS.map(x => [x.symbol, x]));
const livePrices = {};
INSTRUMENTS.forEach(x => livePrices[x.symbol] = x.start);

function getBidAsk(symbol, mid) {
  const inst = INST_MAP[symbol];
  if (!inst) return { bid: mid, ask: mid };
  const half = (mid * CATS[inst.cat].spreadPct) / 2;
  return { bid: mid - half, ask: mid + half };
}

// ---------- DB ----------
let pool, sqlite;
async function initDB() {
  if (USE_PG) {
    const { Pool } = require('pg');
    pool = new Pool({ connectionString: DATABASE_URL, ssl: DATABASE_URL.includes('localhost') ? false : { rejectUnauthorized: false } });
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY,email TEXT UNIQUE NOT NULL,username TEXT UNIQUE NOT NULL,password_hash TEXT NOT NULL,is_admin INTEGER DEFAULT 0,totp_secret TEXT,totp_enabled INTEGER DEFAULT 0,created_at INTEGER DEFAULT (EXTRACT(EPOCH FROM NOW())::INTEGER));
      CREATE TABLE IF NOT EXISTS accounts (user_id INTEGER PRIMARY KEY,balance DOUBLE PRECISION DEFAULT 100000);
      CREATE TABLE IF NOT EXISTS positions (id SERIAL PRIMARY KEY,user_id INTEGER NOT NULL,symbol TEXT NOT NULL,side TEXT NOT NULL,size DOUBLE PRECISION NOT NULL,entry_price DOUBLE PRECISION NOT NULL,sl DOUBLE PRECISION,tp DOUBLE PRECISION,status TEXT NOT NULL DEFAULT 'open',opened_at INTEGER,closed_at INTEGER,close_price DOUBLE PRECISION,close_reason TEXT,pnl DOUBLE PRECISION);
      CREATE TABLE IF NOT EXISTS pending_orders (id SERIAL PRIMARY KEY,user_id INTEGER NOT NULL,symbol TEXT NOT NULL,type TEXT NOT NULL,price DOUBLE PRECISION NOT NULL,size DOUBLE PRECISION NOT NULL,sl DOUBLE PRECISION,tp DOUBLE PRECISION,status TEXT NOT NULL DEFAULT 'pending',created_at INTEGER);
    `);
  } else {
    const Database = require('better-sqlite3');
    const dir = process.env.DATA_DIR || path.join(__dirname, 'data');
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    sqlite = new Database(path.join(dir, 'tradehub.db'));
    sqlite.pragma('journal_mode = WAL');
    sqlite.exec(`
      CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT,email TEXT UNIQUE NOT NULL,username TEXT UNIQUE NOT NULL,password_hash TEXT NOT NULL,is_admin INTEGER DEFAULT 0,totp_secret TEXT,totp_enabled INTEGER DEFAULT 0,created_at INTEGER DEFAULT (strftime('%s','now')));
      CREATE TABLE IF NOT EXISTS accounts (user_id INTEGER PRIMARY KEY,balance REAL DEFAULT 100000);
      CREATE TABLE IF NOT EXISTS positions (id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,symbol TEXT NOT NULL,side TEXT NOT NULL,size REAL NOT NULL,entry_price REAL NOT NULL,sl REAL,tp REAL,status TEXT NOT NULL DEFAULT 'open',opened_at INTEGER,closed_at INTEGER,close_price REAL,close_reason TEXT,pnl REAL);
      CREATE TABLE IF NOT EXISTS pending_orders (id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,symbol TEXT NOT NULL,type TEXT NOT NULL,price REAL NOT NULL,size REAL NOT NULL,sl REAL,tp REAL,status TEXT NOT NULL DEFAULT 'pending',created_at INTEGER);
    `);
  }
}
function toPg(sql) { let i = 0; return sql.replace(/\?/g, () => `$${++i}`); }
async function query(sql, p = []) { return USE_PG ? (await pool.query(toPg(sql), p)).rows : sqlite.prepare(sql).all(...p); }
async function getOne(sql, p = []) { return (await query(sql, p))[0]; }
async function run(sql, p = []) { return USE_PG ? pool.query(toPg(sql), p) : sqlite.prepare(sql).run(...p); }
async function insertId(sql, p = []) {
  if (USE_PG) return (await pool.query(toPg(sql) + ' RETURNING id', p)).rows[0].id;
  return sqlite.prepare(sql).run(...p).lastInsertRowid;
}

// ---------- AUTH ----------
const hashPassword = pw => bcrypt.hashSync(pw, 12);
const verifyPassword = (pw, h) => bcrypt.compareSync(pw, h);
const signToken = u => jwt.sign({ uid: u.id }, JWT_SECRET, { expiresIn: '30d' });
const signTemp = uid => jwt.sign({ uid, p: '2fa' }, JWT_SECRET, { expiresIn: '5m' });

async function authRequired(req, res, next) {
  const token = req.cookies?.token || (req.headers.authorization || '').replace('Bearer ', '');
  if (!token) return res.status(401).json({ error: 'Not authenticated' });
  try {
    const p = jwt.verify(token, JWT_SECRET);
    const user = await getOne('SELECT id,email,username,is_admin,totp_enabled FROM users WHERE id=?', [p.uid]);
    if (!user) return res.status(401).json({ error: 'User not found' });
    req.user = user;
    next();
  } catch { res.status(401).json({ error: 'Invalid token' }); }
}
const adminRequired = (req, res, next) => req.user?.is_admin ? next() : res.status(403).json({ error: 'Admin only' });

// ---------- TRADING ----------
async function usedMargin(userId) {
  const positions = await query('SELECT * FROM positions WHERE user_id=? AND status=?', [userId, 'open']);
  let m = 0;
  for (const p of positions) {
    const inst = INST_MAP[p.symbol];
    if (!inst) continue;
    m += (p.entry_price * p.size) / CATS[inst.cat].lev;
  }
  return m;
}
async function unrealizedPnl(userId) {
  const positions = await query('SELECT * FROM positions WHERE user_id=? AND status=?', [userId, 'open']);
  let total = 0;
  for (const p of positions) {
    const mid = livePrices[p.symbol];
    if (!mid) continue;
    const { bid, ask } = getBidAsk(p.symbol, mid);
    const closePrice = p.side === 'long' ? bid : ask;
    total += (closePrice - p.entry_price) * p.size * (p.side === 'long' ? 1 : -1);
  }
  return total;
}
async function accountStats(userId) {
  const acct = await getOne('SELECT balance FROM accounts WHERE user_id=?', [userId]);
  const balance = acct?.balance ?? 0;
  const unreal = await unrealizedPnl(userId);
  const used = await usedMargin(userId);
  const equity = balance + unreal;
  const freeMargin = equity - used;
  const marginLevel = used > 0 ? (equity / used) * 100 : null;
  return { balance, equity, unrealized: unreal, usedMargin: used, freeMargin, marginLevel };
}
async function openMarket(userId, symbol, side, size, sl, tp) {
  const inst = INST_MAP[symbol];
  if (!inst) throw new Error('Unknown symbol');
  if (!['long', 'short'].includes(side)) throw new Error('Invalid side');
  if (!(size > 0)) throw new Error('Invalid size');
  const mid = livePrices[symbol];
  if (!mid) throw new Error('No price');
  const { bid, ask } = getBidAsk(symbol, mid);
  const entry = side === 'long' ? ask : bid;
  const margin = (entry * size) / CATS[inst.cat].lev;
  const stats = await accountStats(userId);
  if (margin > stats.freeMargin) throw new Error('Insufficient margin');
  if (sl && side === 'long' && sl >= entry) throw new Error('SL must be below entry');
  if (sl && side === 'short' && sl <= entry) throw new Error('SL must be above entry');
  if (tp && side === 'long' && tp <= entry) throw new Error('TP must be above entry');
  if (tp && side === 'short' && tp >= entry) throw new Error('TP must be below entry');
  const id = await insertId(`INSERT INTO positions (user_id,symbol,side,size,entry_price,sl,tp,status,opened_at) VALUES (?,?,?,?,?,?,?,?,?)`,
    [userId, symbol, side, size, entry, sl || null, tp || null, 'open', Math.floor(Date.now() / 1000)]);
  return { id, entry, bid, ask };
}
async function closePosition(pos, price, reason) {
  const pnl = (price - pos.entry_price) * pos.size * (pos.side === 'long' ? 1 : -1);
  await run('UPDATE positions SET status=?,closed_at=?,close_price=?,close_reason=?,pnl=? WHERE id=?',
    ['closed', Math.floor(Date.now() / 1000), price, reason, pnl, pos.id]);
  await run('UPDATE accounts SET balance = balance + ? WHERE user_id=?', [pnl, pos.user_id]);
  return pnl;
}

// ---------- TICK PROCESSOR ----------
async function processTicks() {
  try {
    const pendings = await query("SELECT * FROM pending_orders WHERE status='pending'");
    for (const p of pendings) {
      const mid = livePrices[p.symbol];
      if (!mid) continue;
      const { bid, ask } = getBidAsk(p.symbol, mid);
      let trigger = false;
      if (p.type === 'buy_limit' && ask <= p.price) trigger = true;
      if (p.type === 'sell_limit' && bid >= p.price) trigger = true;
      if (p.type === 'buy_stop' && ask >= p.price) trigger = true;
      if (p.type === 'sell_stop' && bid <= p.price) trigger = true;
      if (!trigger) continue;
      const side = p.type.startsWith('buy') ? 'long' : 'short';
      const entry = side === 'long' ? ask : bid;
      const inst = INST_MAP[p.symbol];
      const margin = (entry * p.size) / CATS[inst.cat].lev;
      const stats = await accountStats(p.user_id);
      if (margin > stats.freeMargin) {
        await run("UPDATE pending_orders SET status='rejected' WHERE id=?", [p.id]);
        continue;
      }
      await run(`INSERT INTO positions (user_id,symbol,side,size,entry_price,sl,tp,status,opened_at) VALUES (?,?,?,?,?,?,?,?,?)`,
        [p.user_id, p.symbol, side, p.size, entry, p.sl, p.tp, 'open', Math.floor(Date.now() / 1000)]);
      await run("UPDATE pending_orders SET status='executed' WHERE id=?", [p.id]);
    }
    const positions = await query("SELECT * FROM positions WHERE status='open'");
    for (const pos of positions) {
      const mid = livePrices[pos.symbol];
      if (!mid) continue;
      const { bid, ask } = getBidAsk(pos.symbol, mid);
      let closeAt = null, reason = null;
      if (pos.side === 'long') {
        if (pos.sl && bid <= pos.sl) { closeAt = pos.sl; reason = 'sl'; }
        else if (pos.tp && bid >= pos.tp) { closeAt = pos.tp; reason = 'tp'; }
      } else {
        if (pos.sl && ask >= pos.sl) { closeAt = pos.sl; reason = 'sl'; }
        else if (pos.tp && ask <= pos.tp) { closeAt = pos.tp; reason = 'tp'; }
      }
      if (closeAt !== null) await closePosition(pos, closeAt, reason);
    }
  } catch (e) { console.error('Tick error:', e.message); }
}

// ---------- CANDLES ----------
function intervalMs(i) {
  const m = { '1m': 60000, '5m': 300000, '15m': 900000, '1h': 3600000, '4h': 14400000, '1d': 86400000 };
  return m[i] || 14400000;
}
function mulberry32(a) {
  return function () {
    a |= 0; a = a + 0x6D2B79F5 | 0;
    var t = Math.imul(a ^ a >>> 15, 1 | a);
    t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t;
    return ((t ^ t >>> 14) >>> 0) / 4294967296;
  };
}
function hashSeed(s) { let h = 0; for (let i = 0; i < s.length; i++) h = ((h << 5) - h + s.charCodeAt(i)) | 0; return Math.abs(h); }

function syntheticCandles(inst, interval, limit) {
  const rand = mulberry32(hashSeed(inst.symbol + interval));
  const out = [];
  let price = inst.start;
  const vol = price * 0.008;
  const ms = intervalMs(interval);
  const now = Date.now();
  for (let i = limit - 1; i >= 0; i--) {
    const o = price + (rand() - 0.5) * vol;
    const c = o + (rand() - 0.5) * vol * 1.2;
    const h = Math.max(o, c) + rand() * vol * 0.6;
    const l = Math.min(o, c) - rand() * vol * 0.6;
    out.push({ t: now - i * ms, o, h, l, c, v: rand() * 100 + 20 });
    price = c;
  }
  if (out.length) {
    const last = out[out.length - 1];
    last.c = livePrices[inst.symbol] || inst.start;
    last.h = Math.max(last.h, last.c);
    last.l = Math.min(last.l, last.c);
  }
  return out;
}
async function fetchKlines(symbol, interval, limit) {
  const inst = INST_MAP[symbol];
  if (!inst) return [];
  if (inst.cat === 'crypto') {
    try {
      const url = `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`;
      const r = await fetch(url);
      if (!r.ok) throw new Error('bad');
      const data = await r.json();
      if (Array.isArray(data) && data.length) {
        return data.map(k => ({ t: k[0], o: +k[1], h: +k[2], l: +k[3], c: +k[4], v: +k[5] }));
      }
    } catch {}
  }
  return syntheticCandles(inst, interval, limit);
}

// ---------- EXPRESS ----------
const app = express();
const server = http.createServer(app);
app.use(express.json());
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/login', (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/admin', (req, res) => res.sendFile(path.join(__dirname, 'public', 'admin.html')));

// --- AUTH ---
app.post('/api/auth/register', async (req, res) => {
  const { email, username, password } = req.body || {};
  if (!email || !username || !password) return res.status(400).json({ error: 'Missing fields' });
  if (password.length < 8) return res.status(400).json({ error: 'Password 8+ chars' });
  if (!/^[a-zA-Z0-9_]{3,20}$/.test(username)) return res.status(400).json({ error: 'Username 3-20 chars, letters/numbers/underscore' });
  const c = await getOne('SELECT COUNT(*) AS c FROM users');
  const isFirst = Number(c.c) === 0 ? 1 : 0;
  try {
    const uid = await insertId('INSERT INTO users (email,username,password_hash,is_admin) VALUES (?,?,?,?)',
      [email.toLowerCase(), username, hashPassword(password), isFirst]);
    await run('INSERT INTO accounts (user_id,balance) VALUES (?,?)', [uid, 100000]);
    const u = await getOne('SELECT * FROM users WHERE id=?', [uid]);
    res.cookie('token', signToken(u), { httpOnly: true, sameSite: 'lax', secure: IS_PROD, maxAge: 30 * 24 * 3600 * 1000 });
    res.json({ ok: true, user: { id: u.id, username: u.username, isAdmin: !!u.is_admin } });
  } catch (e) {
    if (String(e).includes('UNIQUE') || String(e).includes('duplicate')) return res.status(409).json({ error: 'Username or email taken' });
    console.error(e); res.status(500).json({ error: 'Server error' });
  }
});

app.post('/api/auth/login', async (req, res) => {
  const { username, password } = req.body || {};
  const u = await getOne('SELECT * FROM users WHERE username=? OR email=?', [username, (username || '').toLowerCase()]);
  if (!u || !verifyPassword(password, u.password_hash)) return res.status(401).json({ error: 'Invalid credentials' });
  if (u.totp_enabled && u.totp_secret) return res.json({ ok: true, requires2FA: true, tempToken: signTemp(u.id) });
  res.cookie('token', signToken(u), { httpOnly: true, sameSite: 'lax', secure: IS_PROD, maxAge: 30 * 24 * 3600 * 1000 });
  res.json({ ok: true, user: { id: u.id, username: u.username, isAdmin: !!u.is_admin } });
});

app.post('/api/auth/verify-2fa', async (req, res) => {
  const { tempToken, code } = req.body || {};
  if (!tempToken || !code) return res.status(400).json({ error: 'Missing' });
  let p; try { p = jwt.verify(tempToken, JWT_SECRET); } catch { return res.status(401).json({ error: 'Expired' }); }
  if (p.p !== '2fa') return res.status(401).json({ error: 'Bad token' });
  const u = await getOne('SELECT * FROM users WHERE id=?', [p.uid]);
  if (!u?.totp_secret) return res.status(401).json({ error: 'User' });
  if (!authenticator.verify({ token: String(code), secret: u.totp_secret })) return res.status(401).json({ error: 'Bad code' });
  res.cookie('token', signToken(u), { httpOnly: true, sameSite: 'lax', secure: IS_PROD, maxAge: 30 * 24 * 3600 * 1000 });
  res.json({ ok: true, user: { id: u.id, username: u.username, isAdmin: !!u.is_admin } });
});

app.post('/api/auth/logout', (req, res) => { res.clearCookie('token'); res.json({ ok: true }); });

app.get('/api/auth/me', authRequired, (req, res) => {
  res.json({ user: { id: req.user.id, email: req.user.email, username: req.user.username, isAdmin: !!req.user.is_admin, totpEnabled: !!req.user.totp_enabled } });
});

app.post('/api/auth/2fa/setup', authRequired, adminRequired, async (req, res) => {
  const secret = authenticator.generateSecret();
  await run('UPDATE users SET totp_secret=?,totp_enabled=0 WHERE id=?', [secret, req.user.id]);
  res.json({ ok: true, secret, otpauth: authenticator.keyuri(req.user.username, 'TradeHub', secret) });
});
app.post('/api/auth/2fa/enable', authRequired, adminRequired, async (req, res) => {
  const { code } = req.body || {};
  const u = await getOne('SELECT totp_secret FROM users WHERE id=?', [req.user.id]);
  if (!u?.totp_secret) return res.status(400).json({ error: 'Run setup first' });
  if (!authenticator.verify({ token: String(code), secret: u.totp_secret })) return res.status(400).json({ error: 'Bad code' });
  await run('UPDATE users SET totp_enabled=1 WHERE id=?', [req.user.id]);
  res.json({ ok: true });
});
app.post('/api/auth/2fa/disable', authRequired, adminRequired, async (req, res) => {
  await run('UPDATE users SET totp_enabled=0,totp_secret=NULL WHERE id=?', [req.user.id]);
  res.json({ ok: true });
});

// --- MARKET DATA ---
app.get('/api/instruments', (req, res) => {
  const out = INSTRUMENTS.map(x => {
    const mid = livePrices[x.symbol] || x.start;
    const { bid, ask } = getBidAsk(x.symbol, mid);
    return { symbol: x.symbol, base: x.base, quote: x.quote, cat: x.cat, bid, ask, mid, leverage: CATS[x.cat].lev };
  });
  res.json({ instruments: out });
});

app.get('/api/klines/:symbol', async (req, res) => {
  const sym = (req.params.symbol || '').toUpperCase();
  const interval = String(req.query.interval || '4h');
  const limit = Math.min(Number(req.query.limit) || 100, 500);
  const candles = await fetchKlines(sym, interval, limit);
  res.json({ candles });
});

// --- ACCOUNT ---
app.get('/api/account', authRequired, async (req, res) => {
  res.json(await accountStats(req.user.id));
});

// --- POSITIONS ---
app.get('/api/positions', authRequired, async (req, res) => {
  const rows = await query("SELECT * FROM positions WHERE user_id=? AND status='open' ORDER BY id DESC", [req.user.id]);
  const out = rows.map(p => {
    const mid = livePrices[p.symbol];
    const { bid, ask } = getBidAsk(p.symbol, mid);
    const closePrice = p.side === 'long' ? bid : ask;
    const pnl = (closePrice - p.entry_price) * p.size * (p.side === 'long' ? 1 : -1);
    const pnlPct = p.entry_price * p.size > 0 ? (pnl / (p.entry_price * p.size)) * 100 : 0;
    return {
      id: p.id, symbol: p.symbol, side: p.side, size: p.size,
      entry: p.entry_price, sl: p.sl, tp: p.tp,
      bid, ask, closePrice, pnl, pnlPct,
      openedAt: p.opened_at, leverage: CATS[INST_MAP[p.symbol].cat].lev
    };
  });
  res.json({ positions: out });
});

app.post('/api/positions', authRequired, async (req, res) => {
  const { symbol, side, size, sl, tp } = req.body || {};
  try {
    const r = await openMarket(req.user.id, (symbol || '').toUpperCase(), side, Number(size), sl ? Number(sl) : null, tp ? Number(tp) : null);
    res.json({ ok: true, ...r });
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.post('/api/positions/:id/close', authRequired, async (req, res) => {
  const pos = await getOne('SELECT * FROM positions WHERE id=? AND user_id=? AND status=?', [req.params.id, req.user.id, 'open']);
  if (!pos) return res.status(404).json({ error: 'Position not found' });
  const mid = livePrices[pos.symbol];
  const { bid, ask } = getBidAsk(pos.symbol, mid);
  const closePrice = pos.side === 'long' ? bid : ask;
  const pnl = await closePosition(pos, closePrice, 'manual');
  res.json({ ok: true, pnl, closePrice });
});

// --- PENDING ORDERS ---
app.get('/api/pending', authRequired, async (req, res) => {
  const rows = await query("SELECT * FROM pending_orders WHERE user_id=? AND status='pending' ORDER BY id DESC", [req.user.id]);
  res.json({ pending: rows });
});

app.post('/api/pending', authRequired, async (req, res) => {
  const { symbol, type, price, size, sl, tp } = req.body || {};
  const sym = (symbol || '').toUpperCase();
  if (!INST_MAP[sym]) return res.status(400).json({ error: 'Unknown symbol' });
  if (!['buy_limit', 'sell_limit', 'buy_stop', 'sell_stop'].includes(type)) return res.status(400).json({ error: 'Invalid type' });
  const p = Number(price), s = Number(size);
  if (!(p > 0) || !(s > 0)) return res.status(400).json({ error: 'Invalid price or size' });
  const id = await insertId(
    `INSERT INTO pending_orders (user_id,symbol,type,price,size,sl,tp,status,created_at) VALUES (?,?,?,?,?,?,?,?,?)`,
    [req.user.id, sym, type, p, s, sl ? Number(sl) : null, tp ? Number(tp) : null, 'pending', Math.floor(Date.now() / 1000)]);
  res.json({ ok: true, id });
});

app.delete('/api/pending/:id', authRequired, async (req, res) => {
  const o = await getOne("SELECT * FROM pending_orders WHERE id=? AND user_id=? AND status='pending'", [req.params.id, req.user.id]);
  if (!o) return res.status(404).json({ error: 'Not found' });
  await run("UPDATE pending_orders SET status='cancelled' WHERE id=?", [o.id]);
  res.json({ ok: true });
});

// --- HISTORY ---
app.get('/api/history', authRequired, async (req, res) => {
  const rows = await query("SELECT * FROM positions WHERE user_id=? AND status='closed' ORDER BY closed_at DESC LIMIT 100", [req.user.id]);
  res.json({ history: rows });
});

// --- ADMIN ---
app.get('/api/admin/users', authRequired, adminRequired, async (req, res) => {
  const users = await query('SELECT id,email,username,is_admin,totp_enabled,created_at FROM users ORDER BY id ASC');
  const accounts = await query('SELECT user_id,balance FROM accounts');
  const acctMap = Object.fromEntries(accounts.map(a => [a.user_id, a.balance]));
  res.json({ users: users.map(u => ({ ...u, balance: acctMap[u.id] || 0 })) });
});
app.post('/api/admin/users/:id/promote', authRequired, adminRequired, async (req, res) => {
  await run('UPDATE users SET is_admin=1 WHERE id=?', [req.params.id]); res.json({ ok: true });
});
app.post('/api/admin/users/:id/demote', authRequired, adminRequired, async (req, res) => {
  if (Number(req.params.id) === req.user.id) return res.status(400).json({ error: "Can't demote yourself" });
  await run('UPDATE users SET is_admin=0 WHERE id=?', [req.params.id]); res.json({ ok: true });
});
app.post('/api/admin/users/:id/credit', authRequired, adminRequired, async (req, res) => {
  const amt = Number(req.body?.amount) || 0;
  if (!amt) return res.status(400).json({ error: 'Amount required' });
  await run('UPDATE accounts SET balance = balance + ? WHERE user_id=?', [amt, req.params.id]);
  res.json({ ok: true });
});
app.get('/api/admin/positions', authRequired, adminRequired, async (req, res) => {
  const rows = await query(`SELECT p.*, u.username FROM positions p JOIN users u ON u.id = p.user_id ORDER BY p.id DESC LIMIT 200`);
  res.json({ positions: rows });
});

// ---------- WS HUB ----------
const wss = new WebSocket.Server({ server, path: '/ws' });
const clients = new Set();
wss.on('connection', ws => {
  clients.add(ws);
  INSTRUMENTS.forEach(i => {
    const mid = livePrices[i.symbol] || i.start;
    try { ws.send(JSON.stringify({ symbol: i.symbol, mid })); } catch {}
  });
  ws.on('close', () => clients.delete(ws));
  ws.on('error', () => clients.delete(ws));
});
function broadcast(obj) {
  const msg = JSON.stringify(obj);
  for (const c of clients) if (c.readyState === 1) try { c.send(msg); } catch {}
}

// ---------- LIVE FEEDS ----------
const CRYPTO = INSTRUMENTS.filter(i => i.cat === 'crypto');
function startBinance() {
  const streams = CRYPTO.map(i => i.symbol.toLowerCase() + '@trade').join('/');
  const url = 'wss://stream.binance.com:9443/stream?streams=' + streams;
  let up;
  try { up = new WebSocket(url); } catch { return; }
  up.on('message', raw => {
    try {
      const e = JSON.parse(raw); const d = e.data || e;
      if (d.s && d.p) { livePrices[d.s] = parseFloat(d.p); broadcast({ symbol: d.s, mid: livePrices[d.s] }); }
    } catch {}
  });
  up.on('close', () => setTimeout(startBinance, 5000));
  up.on('error', () => {});
}
function startSimulator() {
  const NON = INSTRUMENTS.filter(i => i.cat !== 'crypto');
  setInterval(() => {
    for (const i of NON) {
      const cur = livePrices[i.symbol] || i.start;
      const vol = cur * 0.0005;
      livePrices[i.symbol] = cur + (Math.random() - 0.5) * vol;
      broadcast({ symbol: i.symbol, mid: livePrices[i.symbol] });
    }
  }, 1500);
}

// ---------- START ----------
(async () => {
  try {
    await initDB();
    server.listen(PORT, () => {
      console.log(`✅ TradeHub HFM-style on port ${PORT}`);
      startBinance();
      startSimulator();
      setInterval(processTicks, 1000);
    });
  } catch (e) { console.error('Startup:', e); process.exit(1); }
})();
