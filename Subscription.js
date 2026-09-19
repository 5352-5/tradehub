// subscriptions.js — dynamic balance-tiered subscription
module.exports = async function(app, h) {
  const { query, getOne, run, insertId, authRequired, adminRequired, USE_PG } = h;

  const PK = USE_PG ? 'SERIAL PRIMARY KEY' : 'INTEGER PRIMARY KEY AUTOINCREMENT';
  const TS_NOW = USE_PG ? '(EXTRACT(EPOCH FROM NOW())::INTEGER)' : '(strftime(\'%s\',\'now\'))';

  // Config
  const RATE_PER_1K_KES = Number(process.env.PRICE_PER_1K_KES || 15);   // KES per $1,000 balance / month
  const MIN_BALANCE = Number(process.env.MIN_BALANCE || 1000);
  const MAX_BALANCE = Number(process.env.MAX_BALANCE || 5000000);
  const FREE_BALANCE = Number(process.env.FREE_BALANCE || 10000);
  const MIN_DAYS = 30;

  // Tables
  await run(`
    CREATE TABLE IF NOT EXISTS subscriptions (
      id ${PK},
      user_id INTEGER NOT NULL,
      balance_limit REAL NOT NULL DEFAULT 10000,
      amount_paid_kes REAL DEFAULT 0,
      started_at INTEGER,
      expires_at INTEGER,
      status TEXT DEFAULT 'active'
    )
  `);
  await run(`
    CREATE TABLE IF NOT EXISTS payment_requests (
      id ${PK},
      user_id INTEGER NOT NULL,
      balance_limit REAL NOT NULL,
      amount_kes REAL NOT NULL,
      method TEXT,
      reference TEXT,
      status TEXT DEFAULT 'pending',
      created_at INTEGER DEFAULT ${TS_NOW},
      reviewed_at INTEGER,
      admin_note TEXT
    )
  `);

  function quoteFor(balance) {
    const b = Math.max(MIN_BALANCE, Math.min(MAX_BALANCE, Math.floor(balance / 100) * 100));
    // Free balance is free
    if (b <= FREE_BALANCE) return { balance: b, price_kes: 0, days: 99999, isFree: true };
    const price = Math.ceil((b / 1000) * RATE_PER_1K_KES);
    return { balance: b, price_kes: price, days: 30, isFree: false };
  }

  async function getActiveSub(userId) {
    const now = Math.floor(Date.now() / 1000);
    return await getOne(
      `SELECT * FROM subscriptions WHERE user_id=? AND status='active' AND expires_at>? ORDER BY id DESC`,
      [userId, now]
    );
  }
  async function isPro(userId) {
    const sub = await getActiveSub(userId);
    if (!sub) return false;
    return sub.balance_limit > FREE_BALANCE;
  }
  async function getBalanceLimit(userId) {
    const sub = await getActiveSub(userId);
    if (!sub) return FREE_BALANCE;
    return sub.balance_limit;
  }
  async function grantSubscription(userId, balanceLimit, amountPaid, days) {
    const now = Math.floor(Date.now() / 1000);
    const existing = await getActiveSub(userId);
    let startFrom = now;
    if (existing && existing.expires_at > now && existing.balance_limit >= balanceLimit) {
      // Upgrade mid-cycle: extend from current expiry
      startFrom = existing.expires_at;
    }
    const expires = startFrom + (days || 30) * 86400;
    await run(`UPDATE subscriptions SET status='expired' WHERE user_id=? AND status='active'`, [userId]);
    await run(
      `INSERT INTO subscriptions (user_id,balance_limit,amount_paid_kes,started_at,expires_at,status)
       VALUES (?,?,?,?,?,?)`,
      [userId, balanceLimit, amountPaid || 0, now, expires, 'active']
    );
    return expires;
  }

  // Free default
  async function ensureDefault(userId) {
    const existing = await getActiveSub(userId);
    if (existing) return;
    await grantSubscription(userId, FREE_BALANCE, 0, 99999);
  }

  // USER ROUTES
  app.get('/api/subscription', authRequired, async (req, res) => {
    await ensureDefault(req.user.id);
    const sub = await getActiveSub(req.user.id);
    const pending = await getOne(
      `SELECT * FROM payment_requests WHERE user_id=? AND status='pending' ORDER BY id DESC`,
      [req.user.id]
    );
    res.json({
      subscription: sub,
      balanceLimit: sub ? sub.balance_limit : FREE_BALANCE,
      isPro: sub ? sub.balance_limit > FREE_BALANCE : false,
      pendingRequest: pending || null,
      paybill: process.env.PAYBILL_NUMBER || '400200',
      paybillAccount: process.env.PAYBILL_ACCOUNT || 'TRADEHUB',
      rate: RATE_PER_1K_KES,
      minBalance: MIN_BALANCE,
      maxBalance: MAX_BALANCE,
      freeBalance: FREE_BALANCE
    });
  });

  app.get('/api/subscription/quote', authRequired, (req, res) => {
    const balance = Number(req.query.balance) || FREE_BALANCE;
    const q = quoteFor(balance);
    res.json(q);
  });

  app.post('/api/subscription/request', authRequired, async (req, res) => {
    const { balance_limit, reference, method } = req.body || {};
    if (!balance_limit || !reference) return res.status(400).json({ error: 'Balance and reference required' });
    const q = quoteFor(balance_limit);
    if (q.isFree) return res.status(400).json({ error: 'Free balance needs no payment' });
    const existing = await getOne(
      `SELECT * FROM payment_requests WHERE user_id=? AND status='pending'`,
      [req.user.id]
    );
    if (existing) return res.status(409).json({ error: 'You already have a pending payment. Wait for approval.' });
    const id = await insertId(
      `INSERT INTO payment_requests (user_id,balance_limit,amount_kes,method,reference,status)
       VALUES (?,?,?,?,?,?)`,
      [req.user.id, q.balance, q.price_kes, method || 'mpesa', String(reference).slice(0, 100), 'pending']
    );
    res.json({ ok: true, id, balance: q.balance, amount: q.price_kes });
  });

  // ADMIN ROUTES
  app.get('/api/admin/payment-requests', authRequired, adminRequired, async (req, res) => {
    const rows = await query(
      `SELECT p.*, u.username, u.email FROM payment_requests p JOIN users u ON u.id=p.user_id
       ORDER BY CASE WHEN p.status='pending' THEN 0 ELSE 1 END, p.id DESC LIMIT 200`
    );
    res.json({ requests: rows });
  });
  app.post('/api/admin/payment-requests/:id/approve', authRequired, adminRequired, async (req, res) => {
    const r = await getOne(`SELECT * FROM payment_requests WHERE id=? AND status='pending'`, [req.params.id]);
    if (!r) return res.status(404).json({ error: 'Not found' });
    const expires = await grantSubscription(r.user_id, r.balance_limit, r.amount_kes, 30);
    await run(
      `UPDATE payment_requests SET status='approved',reviewed_at=?,admin_note=? WHERE id=?`,
      [Math.floor(Date.now() / 1000), String(req.body?.note || '').slice(0, 300), r.id]
    );
    res.json({ ok: true, expiresAt: expires });
  });
  app.post('/api/admin/payment-requests/:id/reject', authRequired, adminRequired, async (req, res) => {
    const r = await getOne(`SELECT * FROM payment_requests WHERE id=? AND status='pending'`, [req.params.id]);
    if (!r) return res.status(404).json({ error: 'Not found' });
    await run(
      `UPDATE payment_requests SET status='rejected',reviewed_at=?,admin_note=? WHERE id=?`,
      [Math.floor(Date.now() / 1000), String(req.body?.note || '').slice(0, 300), r.id]
    );
    res.json({ ok: true });
  });

  return { isPro, getActiveSub, getBalanceLimit, grantSubscription, ensureDefault, FREE_BALANCE };
};
