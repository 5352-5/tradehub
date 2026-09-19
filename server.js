const express=require('express');
const cookieParser=require('cookie-parser');
const http=require('http');
const WebSocket=require('ws');
const jwt=require('jsonwebtoken');
const bcrypt=require('bcryptjs');
const path=require('path');
const fs=require('fs');
const crypto=require('crypto');
const {authenticator}=require('otplib');

const PORT=process.env.PORT||3000;
const JWT_SECRET=process.env.JWT_SECRET||crypto.randomBytes(48).toString('hex');
const DATABASE_URL=process.env.DATABASE_URL;
const USE_PG=!!DATABASE_URL;
const IS_PROD=process.env.NODE_ENV==='production';

const RATE_PER_1K_KES=Number(process.env.PRICE_PER_1K_KES||15);
const MIN_BALANCE=Number(process.env.MIN_BALANCE||1000);
const MAX_BALANCE=Number(process.env.MAX_BALANCE||5000000);
const FREE_BALANCE=Number(process.env.FREE_BALANCE||10000);
const PAYBILL_NUMBER=process.env.PAYBILL_NUMBER||'400200';
const PAYBILL_ACCOUNT=process.env.PAYBILL_ACCOUNT||'TRADEHUB';

const CATS={
  crypto:{label:'Crypto',lev:10,spreadPct:0.0004},
  forex:{label:'Forex',lev:100,spreadPct:0.00008},
  commodities:{label:'Commodities',lev:50,spreadPct:0.0005},
  stocks:{label:'Stocks',lev:20,spreadPct:0.0006},
  indices:{label:'Indices',lev:50,spreadPct:0.0003}
};

const INSTRUMENTS=[
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
].map(([symbol,base,quote,start,cat])=>({symbol,base,quote,start,cat}));

const INST_MAP=Object.fromEntries(INSTRUMENTS.map(x=>[x.symbol,x]));
const livePrices={};
INSTRUMENTS.forEach(x=>livePrices[x.symbol]=x.start);

function getBidAsk(symbol,mid){
  const inst=INST_MAP[symbol];
  if(!inst)return{bid:mid,ask:mid};
  const half=(mid*CATS[inst.cat].spreadPct)/2;
  return{bid:mid-half,ask:mid+half};
}

let pool,sqlite;
async function initDB(){
  if(USE_PG){
    const {Pool}=require('pg');
    pool=new Pool({connectionString:DATABASE_URL,ssl:DATABASE_URL.includes('localhost')?false:{rejectUnauthorized:false}});
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users(id SERIAL PRIMARY KEY,email TEXT UNIQUE NOT NULL,username TEXT UNIQUE NOT NULL,password_hash TEXT NOT NULL,is_admin INTEGER DEFAULT 0,totp_secret TEXT,totp_enabled INTEGER DEFAULT 0,created_at INTEGER DEFAULT (EXTRACT(EPOCH FROM NOW())::INTEGER));
      CREATE TABLE IF NOT EXISTS accounts(user_id INTEGER PRIMARY KEY,balance DOUBLE PRECISION DEFAULT 0);
      CREATE TABLE IF NOT EXISTS positions(id SERIAL PRIMARY KEY,user_id INTEGER NOT NULL,symbol TEXT NOT NULL,side TEXT NOT NULL,size DOUBLE PRECISION NOT NULL,entry_price DOUBLE PRECISION NOT NULL,sl DOUBLE PRECISION,tp DOUBLE PRECISION,status TEXT NOT NULL DEFAULT 'open',opened_at INTEGER,closed_at INTEGER,close_price DOUBLE PRECISION,close_reason TEXT,pnl DOUBLE PRECISION);
      CREATE TABLE IF NOT EXISTS pending_orders(id SERIAL PRIMARY KEY,user_id INTEGER NOT NULL,symbol TEXT NOT NULL,type TEXT NOT NULL,price DOUBLE PRECISION NOT NULL,size DOUBLE PRECISION NOT NULL,sl DOUBLE PRECISION,tp DOUBLE PRECISION,status TEXT NOT NULL DEFAULT 'pending',created_at INTEGER);
      CREATE TABLE IF NOT EXISTS subscriptions(id SERIAL PRIMARY KEY,user_id INTEGER NOT NULL,balance_limit DOUBLE PRECISION NOT NULL DEFAULT 10000,amount_paid_kes DOUBLE PRECISION DEFAULT 0,started_at INTEGER,expires_at INTEGER,status TEXT DEFAULT 'active');
      CREATE TABLE IF NOT EXISTS payment_requests(id SERIAL PRIMARY KEY,user_id INTEGER NOT NULL,balance_limit DOUBLE PRECISION NOT NULL,amount_kes DOUBLE PRECISION NOT NULL,method TEXT,reference TEXT,status TEXT DEFAULT 'pending',created_at INTEGER DEFAULT (EXTRACT(EPOCH FROM NOW())::INTEGER),reviewed_at INTEGER,admin_note TEXT);
    `);
  }else{
    const Database=require('better-sqlite3');
    const dir=process.env.DATA_DIR||path.join(__dirname,'data');
    if(!fs.existsSync(dir))fs.mkdirSync(dir,{recursive:true});
    sqlite=new Database(path.join(dir,'tradehub.db'));
    sqlite.pragma('journal_mode = WAL');
    sqlite.exec(`
      CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY AUTOINCREMENT,email TEXT UNIQUE NOT NULL,username TEXT UNIQUE NOT NULL,password_hash TEXT NOT NULL,is_admin INTEGER DEFAULT 0,totp_secret TEXT,totp_enabled INTEGER DEFAULT 0,created_at INTEGER DEFAULT (strftime('%s','now')));
      CREATE TABLE IF NOT EXISTS accounts(user_id INTEGER PRIMARY KEY,balance REAL DEFAULT 0);
      CREATE TABLE IF NOT EXISTS positions(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,symbol TEXT NOT NULL,side TEXT NOT NULL,size REAL NOT NULL,entry_price REAL NOT NULL,sl REAL,tp REAL,status TEXT NOT NULL DEFAULT 'open',opened_at INTEGER,closed_at INTEGER,close_price REAL,close_reason TEXT,pnl REAL);
      CREATE TABLE IF NOT EXISTS pending_orders(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,symbol TEXT NOT NULL,type TEXT NOT NULL,price REAL NOT NULL,size REAL NOT NULL,sl REAL,tp REAL,status TEXT NOT NULL DEFAULT 'pending',created_at INTEGER);
      CREATE TABLE IF NOT EXISTS subscriptions(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,balance_limit REAL NOT NULL DEFAULT 10000,amount_paid_kes REAL DEFAULT 0,started_at INTEGER,expires_at INTEGER,status TEXT DEFAULT 'active');
      CREATE TABLE IF NOT EXISTS payment_requests(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,balance_limit REAL NOT NULL,amount_kes REAL NOT NULL,method TEXT,reference TEXT,status TEXT DEFAULT 'pending',created_at INTEGER DEFAULT (strftime('%s','now')),reviewed_at INTEGER,admin_note TEXT);
    `);
  }
}
function toPg(sql){let i=0;return sql.replace(/\?/g,()=>`$${++i}`)}
async function query(sql,p=[]){return USE_PG?(await pool.query(toPg(sql),p)).rows:sqlite.prepare(sql).all(...p)}
async function getOne(sql,p=[]){return(await query(sql,p))[0]}
async function run(sql,p=[]){return USE_PG?pool.query(toPg(sql),p):sqlite.prepare(sql).run(...p)}
async function insertId(sql,p=[]){
  if(USE_PG)return(await pool.query(toPg(sql)+' RETURNING id',p)).rows[0].id;
  return sqlite.prepare(sql).run(...p).lastInsertRowid;
}

const hashPassword=pw=>bcrypt.hashSync(pw,12);
const verifyPassword=(pw,h)=>bcrypt.compareSync(pw,h);
const signToken=u=>jwt.sign({uid:u.id},JWT_SECRET,{expiresIn:'30d'});
const signTemp=uid=>jwt.sign({uid,p:'2fa'},JWT_SECRET,{expiresIn:'5m'});

async function authRequired(req,res,next){
  const token=req.cookies?.token||(req.headers.authorization||'').replace('Bearer ','');
  if(!token)return res.status(401).json({error:'Not authenticated'});
  try{
    const p=jwt.verify(token,JWT_SECRET);
    const user=await getOne('SELECT id,email,username,is_admin,totp_enabled FROM users WHERE id=?',[p.uid]);
    if(!user)return res.status(401).json({error:'User not found'});
    req.user=user;next();
  }catch{res.status(401).json({error:'Invalid token'})}
}
const adminRequired=(req,res,next)=>req.user?.is_admin?next():res.status(403).json({error:'Admin only'});

async function usedMargin(userId){
  const positions=await query('SELECT * FROM positions WHERE user_id=? AND status=?',[userId,'open']);
  let m=0;
  for(const p of positions){
    const inst=INST_MAP[p.symbol];if(!inst)continue;
    m+=(p.entry_price*p.size)/CATS[inst.cat].lev;
  }
  return m;
}
async function unrealizedPnl(userId){
  const positions=await query('SELECT * FROM positions WHERE user_id=? AND status=?',[userId,'open']);
  let total=0;
  for(const p of positions){
    const mid=livePrices[p.symbol];if(!mid)continue;
    const{bid,ask}=getBidAsk(p.symbol,mid);
    const closePrice=p.side==='long'?bid:ask;
    total+=(closePrice-p.entry_price)*p.size*(p.side==='long'?1:-1);
  }
  return total;
}

// ---------- SUBSCRIPTION HELPERS ----------
function quoteFor(balance){
  const b=Math.max(MIN_BALANCE,Math.min(MAX_BALANCE,Math.floor(balance/100)*100));
  if(b<=FREE_BALANCE)return{balance:b,price_kes:0,days:99999,isFree:true};
  const price=Math.ceil((b/1000)*RATE_PER_1K_KES);
  return{balance:b,price_kes:price,days:30,isFree:false};
}
async function getActiveSub(userId){
  const now=Math.floor(Date.now()/1000);
  return await getOne(`SELECT * FROM subscriptions WHERE user_id=? AND status='active' AND expires_at>? ORDER BY id DESC`,[userId,now]);
}
async function isPro(userId){
  const sub=await getActiveSub(userId);
  return !!sub&&sub.balance_limit>FREE_BALANCE;
}
async function getBalanceLimit(userId){
  const sub=await getActiveSub(userId);
  return sub?sub.balance_limit:FREE_BALANCE;
}
async function grantSubscription(userId,balanceLimit,amountPaid,days){
  const now=Math.floor(Date.now()/1000);
  const existing=await getActiveSub(userId);
  let startFrom=now;
  if(existing&&existing.expires_at>now&&existing.balance_limit>=balanceLimit){
    startFrom=existing.expires_at;
  }
  const expires=startFrom+(days||30)*86400;
  await run(`UPDATE subscriptions SET status='expired' WHERE user_id=? AND status='active'`,[userId]);
  await run(`INSERT INTO subscriptions(user_id,balance_limit,amount_paid_kes,started_at,expires_at,status) VALUES(?,?,?,?,?,?)`,[userId,balanceLimit,amountPaid||0,now,expires,'active']);
  return expires;
}
async function ensureDefaultSub(userId){
  const existing=await getActiveSub(userId);
  if(existing)return;
  await grantSubscription(userId,FREE_BALANCE,0,99999);
}

async function accountStats(userId){
  const acct=await getOne('SELECT balance FROM accounts WHERE user_id=?',[userId]);
  const balance=acct?.balance??0;
  const unreal=await unrealizedPnl(userId);
  const used=await usedMargin(userId);
  const equity=balance+unreal;
  const freeMargin=equity-used;
  const marginLevel=used>0?(equity/used)*100:null;
  let balanceLimit=FREE_BALANCE;
  try{balanceLimit=await getBalanceLimit(userId)}catch{}
  return{balance,equity,unrealized:unreal,usedMargin:used,freeMargin,marginLevel,balanceLimit};
}
async function openMarket(userId,symbol,side,size,sl,tp){
  const inst=INST_MAP[symbol];
  if(!inst)throw new Error('Unknown symbol');
  if(!['long','short'].includes(side))throw new Error('Invalid side');
  if(!(size>0))throw new Error('Invalid size');
  const mid=livePrices[symbol];
  if(!mid)throw new Error('No price');
  const{bid,ask}=getBidAsk(symbol,mid);
  const entry=side==='long'?ask:bid;
  const margin=(entry*size)/CATS[inst.cat].lev;
  const stats=await accountStats(userId);
  if(margin>stats.freeMargin)throw new Error('Insufficient free margin');
  if(sl&&side==='long'&&sl>=entry)throw new Error('SL must be below entry');
  if(sl&&side==='short'&&sl<=entry)throw new Error('SL must be above entry');
  if(tp&&side==='long'&&tp<=entry)throw new Error('TP must be above entry');
  if(tp&&side==='short'&&tp>=entry)throw new Error('TP must be below entry');
  const id=await insertId(`INSERT INTO positions(user_id,symbol,side,size,entry_price,sl,tp,status,opened_at) VALUES(?,?,?,?,?,?,?,?,?)`,[userId,symbol,side,size,entry,sl||null,tp||null,'open',Math.floor(Date.now()/1000)]);
  return{id,entry,bid,ask};
}
async function closePosition(pos,price,reason){
  const pnl=(price-pos.entry_price)*pos.size*(pos.side==='long'?1:-1);
  await run('UPDATE positions SET status=?,closed_at=?,close_price=?,close_reason=?,pnl=? WHERE id=?',['closed',Math.floor(Date.now()/1000),price,reason,pnl,pos.id]);
  await run('UPDATE accounts SET balance=balance+? WHERE user_id=?',[pnl,pos.user_id]);
  return pnl;
}

async function processTicks(){
  try{
    const pendings=await query("SELECT * FROM pending_orders WHERE status='pending'");
    for(const p of pendings){
      const mid=livePrices[p.symbol];if(!mid)continue;
      const{bid,ask}=getBidAsk(p.symbol,mid);
      let trigger=false;
      if(p.type==='buy_limit'&&ask<=p.price)trigger=true;
      if(p.type==='sell_limit'&&bid>=p.price)trigger=true;
      if(p.type==='buy_stop'&&ask>=p.price)trigger=true;
      if(p.type==='sell_stop'&&bid<=p.price)trigger=true;
      if(!trigger)continue;
      const side=p.type.startsWith('buy')?'long':'short';
      const entry=side==='long'?ask:bid;
      const inst=INST_MAP[p.symbol];
      const margin=(entry*p.size)/CATS[inst.cat].lev;
      const stats=await accountStats(p.user_id);
      if(margin>stats.freeMargin){await run("UPDATE pending_orders SET status='rejected' WHERE id=?",[p.id]);continue}
      await run(`INSERT INTO positions(user_id,symbol,side,size,entry_price,sl,tp,status,opened_at) VALUES(?,?,?,?,?,?,?,?,?)`,[p.user_id,p.symbol,side,p.size,entry,p.sl,p.tp,'open',Math.floor(Date.now()/1000)]);
      await run("UPDATE pending_orders SET status='executed' WHERE id=?",[p.id]);
    }
    const positions=await query("SELECT * FROM positions WHERE status='open'");
    for(const pos of positions){
      const mid=livePrices[pos.symbol];if(!mid)continue;
      const{bid,ask}=getBidAsk(pos.symbol,mid);
      let closeAt=null,reason=null;
      if(pos.side==='long'){
        if(pos.sl&&bid<=pos.sl){closeAt=pos.sl;reason='sl'}
        else if(pos.tp&&bid>=pos.tp){closeAt=pos.tp;reason='tp'}
      }else{
        if(pos.sl&&ask>=pos.sl){closeAt=pos.sl;reason='sl'}
        else if(pos.tp&&ask<=pos.tp){closeAt=pos.tp;reason='tp'}
      }
      if(closeAt!==null)await closePosition(pos,closeAt,reason);
    }
  }catch(e){console.error('Tick error:',e.message)}
}

function intervalMs(i){return{'1m':60000,'5m':300000,'15m':900000,'1h':3600000,'4h':14400000,'1d':86400000}[i]||14400000}
function mulberry32(a){return function(){a|=0;a=a+0x6D2B79F5|0;var t=Math.imul(a^a>>>15,1|a);t=t+Math.imul(t^t>>>7,61|t)^t;return((t^t>>>14)>>>0)/4294967296}}
function hashSeed(s){let h=0;for(let i=0;i<s.length;i++)h=((h<<5)-h+s.charCodeAt(i))|0;return Math.abs(h)}

function syntheticCandles(inst,interval,limit){
  const rand=mulberry32(hashSeed(inst.symbol+interval));
  const out=[];
  let price=inst.start;
  const vol=price*0.0003;
  const ms=intervalMs(interval);
  const now=Date.now();
  for(let i=limit-1;i>=0;i--){
    const o=price+(rand()-0.5)*vol;
    const c=o+(rand()-0.5)*vol*1.2;
    const h=Math.max(o,c)+rand()*vol*0.6;
    const l=Math.min(o,c)-rand()*vol*0.6;
    out.push({t:now-i*ms,o,h,l,c,v:rand()*100+20});
    price=c;
  }
  if(out.length){
    const live=livePrices[inst.symbol]||inst.start;
    const offset=live-out[out.length-1].c;
    for(const c of out){c.o+=offset;c.h+=offset;c.l+=offset;c.c+=offset}
  }
  return out;
}
async function // ---------- TECHNICAL INDICATORS ----------
function calcRSI(closes,period){
  if(closes.length<period+1)return null;
  let gains=0,losses=0;
  for(let i=1;i<=period;i++){
    const diff=closes[i]-closes[i-1];
    if(diff>=0)gains+=diff;else losses-=diff;
  }
  let avgGain=gains/period,avgLoss=losses/period;
  for(let i=period+1;i<closes.length;i++){
    const diff=closes[i]-closes[i-1];
    const g=diff>0?diff:0,l=diff<0?-diff:0;
    avgGain=(avgGain*(period-1)+g)/period;
    avgLoss=(avgLoss*(period-1)+l)/period;
  }
  if(avgLoss===0)return 100;
  const rs=avgGain/avgLoss;
  return 100-100/(1+rs);
}
function calcEMA(values,period){
  if(values.length<period)return null;
  const k=2/(period+1);
  let ema=values.slice(0,period).reduce((a,b)=>a+b,0)/period;
  for(let i=period;i<values.length;i++)ema=values[i]*k+ema*(1-k);
  return ema;
}
function calcEMAseries(values,period){
  if(values.length<period)return [];
  const k=2/(period+1);
  const out=[];
  let ema=values.slice(0,period).reduce((a,b)=>a+b,0)/period;
  out.push({i:period-1,v:ema});
  for(let i=period;i<values.length;i++){ema=values[i]*k+ema*(1-k);out.push({i,v:ema})}
  return out;
}
function calcMACD(closes){
  const ema12=calcEMAseries(closes,12);
  const ema26=calcEMAseries(closes,26);
  if(!ema12.length||!ema26.length)return null;
  const map26=Object.fromEntries(ema26.map(x=>[x.i,x.v]));
  const macdLine=[];
  for(const e of ema12)if(map26[e.i]!=null)macdLine.push({i:e.i,v:e.v-map26[e.i]});
  if(macdLine.length<9)return null;
  const vals=macdLine.map(x=>x.v);
  const signal=calcEMA(vals,9);
  const macd=vals[vals.length-1];
  const prev=vals[vals.length-2];
  const prevSignal=calcEMA(vals.slice(0,-1),9);
  return{macd,signal,prev,prevSignal,histogram:macd-signal,prevHistogram:prev-prevSignal};
}
function calcBollinger(closes,period,mult){
  if(closes.length<period)return null;
  const slice=closes.slice(-period);
  const mean=slice.reduce((a,b)=>a+b,0)/period;
  const variance=slice.reduce((a,b)=>a+Math.pow(b-mean,2),0)/period;
  const sd=Math.sqrt(variance);
  return{upper:mean+mult*sd,middle:mean,lower:mean-mult*sd,sd};
}

function computeSignal(closes){
  if(!closes||closes.length<30)return null;
  const price=closes[closes.length-1];
  const rsi=calcRSI(closes,14);
  const ema9=calcEMA(closes,9);
  const ema21=calcEMA(closes,21);
  const macd=calcMACD(closes);
  const bb=calcBollinger(closes,20,2);

  let score=0;
  const parts=[];

  // RSI
  if(rsi!=null){
    if(rsi<30){score+=2;parts.push({name:'RSI',value:rsi.toFixed(1),signal:'buy',note:'Oversold'})}
    else if(rsi>70){score-=2;parts.push({name:'RSI',value:rsi.toFixed(1),signal:'sell',note:'Overbought'})}
    else parts.push({name:'RSI',value:rsi.toFixed(1),signal:'neutral',note:'Normal'});
  }

  // EMA cross
  if(ema9!=null&&ema21!=null){
    const diff=(ema9-ema21)/ema21;
    if(diff>0.0005){score+=2;parts.push({name:'EMA 9/21',value:'Bullish',signal:'buy',note:'Golden cross'})}
    else if(diff<-0.0005){score-=2;parts.push({name:'EMA 9/21',value:'Bearish',signal:'sell',note:'Death cross'})}
    else parts.push({name:'EMA 9/21',value:'Flat',signal:'neutral',note:'No clear trend'});
  }

  // MACD
  if(macd){
    if(macd.macd>macd.signal&&macd.prev<=macd.prevSignal){score+=2;parts.push({name:'MACD',value:'Cross up',signal:'buy',note:'Bullish crossover'})}
    else if(macd.macd<macd.signal&&macd.prev>=macd.prevSignal){score-=2;parts.push({name:'MACD',value:'Cross down',signal:'sell',note:'Bearish crossover'})}
    else if(macd.histogram>0){score+=1;parts.push({name:'MACD',value:'Above',signal:'buy',note:'Momentum up'})}
    else if(macd.histogram<0){score-=1;parts.push({name:'MACD',value:'Below',signal:'sell',note:'Momentum down'})}
  }

  // Bollinger
  if(bb){
    if(price>bb.upper){score-=1;parts.push({name:'Bollinger',value:'Above upper',signal:'sell',note:'Overextended'})}
    else if(price<bb.lower){score+=1;parts.push({name:'Bollinger',value:'Below lower',signal:'buy',note:'Overextended'})}
    else parts.push({name:'Bollinger',value:'Inside',signal:'neutral',note:'Within bands'});
  }

  let overall='NEUTRAL',cls='neutral';
  if(score>=4){overall='STRONG BUY';cls='strong-buy'}
  else if(score>=2){overall='BUY';cls='buy'}
  else if(score<=-4){overall='STRONG SELL';cls='strong-sell'}
  else if(score<=-2){overall='SELL';cls='sell'}

  return{overall,cls,score,parts,price};
}fetchKlines(symbol,interval,limit){
  const inst=INST_MAP[symbol];
  if(!inst)return[];
  if(inst.cat==='crypto'){
    try{
      const r=await fetch(`https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`);
      if(r.ok){
        const data=await r.json();
        if(Array.isArray(data)&&data.length){
          const mapped=data.map(k=>({t:k[0],o:+k[1],h:+k[2],l:+k[3],c:+k[4],v:+k[5]}));
          if(mapped.length){
            const live=livePrices[inst.symbol]||inst.start;
            const offset=live-mapped[mapped.length-1].c;
            if(Math.abs(offset)/live<0.05){
              for(const c of mapped){c.o+=offset;c.h+=offset;c.l+=offset;c.c+=offset}
            }
          }
          return mapped;
        }
      }
    }catch{}
  }
  return syntheticCandles(inst,interval,limit);
}

const app=express();
const server=http.createServer(app);
app.use(express.json());
app.use(cookieParser());

const BASE_CSS=`*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}:root{--bg:#0b0e11;--panel:#161a20;--panel2:#1e2329;--hover:#2b3139;--border:#262b33;--text:#eaecef;--text2:#848e9c;--text3:#5e6673;--yellow:#f0b90b;--yellow2:#d4a30a;--green:#0ecb81;--green-bg:rgba(14,203,129,.12);--red:#f6465d;--red-bg:rgba(246,70,93,.12);--blue:#1e6cf5}html,body{height:100%}body{background:var(--bg);color:var(--text);font-family:'Inter',system-ui,-apple-system,sans-serif;font-size:13px;-webkit-font-smoothing:antialiased}button{font-family:inherit;cursor:pointer;border:none;border-radius:8px;color:var(--text);transition:opacity .15s}button:active{opacity:.7}input,select,textarea{font-family:inherit;background:var(--panel2);border:1px solid var(--border);border-radius:10px;padding:14px;color:var(--text);font-size:15px;outline:none;width:100%;font-weight:500}input:focus,select:focus,textarea:focus{border-color:var(--yellow)}.up{color:var(--green)}.down{color:var(--red)}.muted{color:var(--text2)}.flex{display:flex}.spacer{flex:1}`;

const LOGIN_HTML=`<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>TradeHub Login</title><style>${BASE_CSS}body{min-height:100dvh;display:flex;align-items:center;justify-content:center;padding:20px;background:radial-gradient(circle at 50% 0%,#1a1f2b 0%,#0b0e11 60%)}.card{background:var(--panel);padding:40px 28px;border-radius:20px;width:100%;max-width:400px;border:1px solid var(--border);box-shadow:0 30px 80px rgba(0,0,0,.6)}.logo{font-size:28px;font-weight:800;color:var(--yellow);text-align:center;letter-spacing:-1px;margin-bottom:6px}.tag{color:var(--text2);font-size:12px;text-align:center;margin-bottom:32px}.tabs{display:flex;background:var(--panel2);border-radius:12px;padding:4px;margin-bottom:24px}.tab{flex:1;padding:10px;text-align:center;font-size:13px;font-weight:600;color:var(--text2);border-radius:8px;background:transparent;border:none;cursor:pointer;font-family:inherit}.tab.active{background:var(--hover);color:var(--text)}form{display:flex;flex-direction:column;gap:14px}label{font-size:11px;color:var(--text2);font-weight:600;display:block;margin-bottom:6px;text-transform:uppercase;letter-spacing:.4px}.primary{background:var(--yellow);color:#0b0e11;font-weight:700;padding:14px;font-size:15px;border-radius:10px;border:none;cursor:pointer;font-family:inherit;margin-top:8px}.primary:hover{background:var(--yellow2)}.err{color:var(--red);font-size:12px;text-align:center;min-height:16px;margin-top:10px}.hint{color:var(--text3);font-size:11px;text-align:center;margin-top:24px;line-height:1.6}</style></head><body><div class="card"><div class="logo">⚡ TradeHub</div><div class="tag">Multi-asset paper trading · HFM-style</div><div class="tabs"><button class="tab active" data-tab="login" type="button">Login</button><button class="tab" data-tab="register" type="button">Register</button></div><form id="loginForm"><div><label>Username or email</label><input name="username" required autocomplete="username"></div><div><label>Password</label><input name="password" type="password" required autocomplete="current-password"></div><button type="submit" class="primary">Log In</button></form><form id="registerForm" style="display:none"><div><label>Email</label><input name="email" type="email" required autocomplete="email"></div><div><label>Username</label><input name="username" required pattern="[a-zA-Z0-9_]{3,20}" autocomplete="username"></div><div><label>Password (8+ chars)</label><input name="password" type="password" required minlength="8" autocomplete="new-password"></div><button type="submit" class="primary">Create Account</button></form><form id="twofaForm" style="display:none"><div><label>2FA code</label><input name="code" inputmode="numeric" maxlength="6" required></div><button type="submit" class="primary">Verify</button></form><div class="err" id="err"></div><div class="hint">Free tier: $10,000 virtual balance.<br>First user becomes admin.</div></div><script>var tabs=document.querySelectorAll('.tab'),lf=document.getElementById('loginForm'),rf=document.getElementById('registerForm'),tf=document.getElementById('twofaForm'),err=document.getElementById('err'),tempToken=null;tabs.forEach(function(t){t.onclick=function(){tabs.forEach(function(x){x.classList.remove('active')});t.classList.add('active');var isL=t.dataset.tab==='login';lf.style.display=isL?'flex':'none';rf.style.display=isL?'none':'flex';tf.style.display='none';err.textContent=''}});async function post(u,d){var r=await fetch(u,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)});return[r.ok,await r.json()]}lf.onsubmit=async function(e){e.preventDefault();var res=await post('/api/auth/login',Object.fromEntries(new FormData(lf)));if(!res[0]){err.textContent=res[1].error;return}if(res[1].requires2FA){tempToken=res[1].tempToken;lf.style.display='none';tf.style.display='flex';return}location.href='/'};tf.onsubmit=async function(e){e.preventDefault();var d=Object.fromEntries(new FormData(tf));d.tempToken=tempToken;var res=await post('/api/auth/verify-2fa',d);if(res[0])location.href='/';else err.textContent=res[1].error};rf.onsubmit=async function(e){e.preventDefault();var res=await post('/api/auth/register',Object.fromEntries(new FormData(rf)));if(res[0])location.href='/';else err.textContent=res[1].error};fetch('/api/auth/me').then(function(r){if(r.ok)location.href='/'})<\/script></body></html>`;

const APP_HTML=`<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no"><title>TradeHub</title><style>${BASE_CSS}body{height:100dvh;display:flex;flex-direction:column;overflow:hidden}.topbar{padding:14px 18px 10px;display:flex;align-items:center;gap:10px;flex-shrink:0}.brand{color:var(--yellow);font-weight:800;font-size:18px}.conn{display:flex;align-items:center;gap:5px;font-size:10px;color:var(--green);background:var(--green-bg);padding:4px 8px;border-radius:20px;font-weight:600}.conn .dot{width:5px;height:5px;background:var(--green);border-radius:50%;animation:p 1.5s infinite}@keyframes p{50%{opacity:.3}}.acct-mini{text-align:right;line-height:1.2}.acct-mini b{font-family:monospace;font-size:15px;color:var(--text);display:block}.acct-mini span{color:var(--text3);font-size:10px;text-transform:uppercase}.pages{flex:1;position:relative;overflow:hidden;min-height:0}.page{position:absolute;inset:0;overflow-y:auto;display:none;-webkit-overflow-scrolling:touch}.page.active{display:block}.cat-tabs{display:flex;gap:8px;overflow-x:auto;padding:8px 18px 14px}.cat-tabs::-webkit-scrollbar{display:none}.cat-tab{flex-shrink:0;padding:9px 16px;font-size:12px;font-weight:600;color:var(--text2);border-radius:20px;cursor:pointer;white-space:nowrap;background:var(--panel);border:1px solid var(--border)}.cat-tab.active{color:#0b0e11;background:var(--yellow);border-color:var(--yellow)}.instr{padding:14px 18px;display:flex;justify-content:space-between;align-items:center;cursor:pointer;border-bottom:1px solid rgba(38,43,51,.4)}.instr:active{background:var(--hover)}.instr .left{display:flex;align-items:center;gap:12px;flex:1;min-width:0}.instr .icon{width:38px;height:38px;border-radius:50%;background:var(--panel2);display:flex;align-items:center;justify-content:center;font-size:16px;color:var(--text2);flex-shrink:0}.instr .name{font-family:monospace;font-size:14px;font-weight:600;white-space:nowrap}.instr .sub{font-size:10px;color:var(--text3);margin-top:2px;text-transform:uppercase}.instr .right{text-align:right;display:flex;flex-direction:column;align-items:flex-end;gap:4px}.instr .price{font-family:monospace;font-size:14px;font-weight:600}.pill{display:inline-flex;align-items:center;gap:3px;font-size:11px;font-weight:700;padding:3px 8px;border-radius:20px;font-family:monospace}.pill.up{background:var(--green-bg);color:var(--green)}.pill.down{background:var(--red-bg);color:var(--red)}.trade-hd{padding:16px 18px}.symline{display:flex;align-items:baseline;gap:10px;margin-bottom:14px}.symbig{font-family:monospace;font-size:20px;font-weight:700}.badge{background:var(--panel2);color:var(--yellow);font-size:10px;padding:3px 8px;border-radius:6px;font-weight:700}.ba-row{display:flex;gap:10px}.ba{flex:1;background:var(--panel);border-radius:14px;padding:14px 12px;text-align:center;border:1px solid var(--border)}.ba .lbl{font-size:10px;color:var(--text3);text-transform:uppercase;font-weight:600;margin-bottom:4px}.ba .val{font-family:monospace;font-size:20px;font-weight:700}.ba.sell .val{color:var(--red)}.ba.buy .val{color:var(--green)}.chartwrap{padding:0 18px}.tf{display:flex;gap:6px;margin-bottom:10px;overflow-x:auto;padding:2px 0}.tf::-webkit-scrollbar{display:none}.tf span{padding:6px 12px;font-size:11px;color:var(--text2);border-radius:8px;cursor:pointer;font-weight:600;flex-shrink:0;background:var(--panel)}.tf span.on{background:var(--hover);color:var(--yellow)}.candles{height:200px;display:flex;align-items:flex-end;gap:2px;padding:10px 60px 8px 10px;position:relative;background:var(--panel);border-radius:14px;border:1px solid var(--border);overflow:hidden}.candle{flex:1;min-width:2px;max-width:16px;display:flex;flex-direction:column;justify-content:flex-end;position:relative}.candle .wick{width:1px;background:#5e6673;position:absolute;left:50%;transform:translateX(-50%)}.candle .body{width:100%;position:relative;z-index:2;border-radius:1px;min-height:1px}.candle.green .body{background:var(--green)}.candle.red .body{background:var(--red)}.paxis{position:absolute;right:0;top:10px;bottom:8px;width:58px;display:flex;flex-direction:column;justify-content:space-between;font-size:10px;font-family:monospace;color:var(--text3);pointer-events:none;border-left:1px solid var(--border)}.paxis span{text-align:right;padding-right:8px}.subtabs{display:flex;gap:6px;padding:16px 18px 12px;overflow-x:auto}.subtabs::-webkit-scrollbar{display:none}.subtabs span{padding:8px 16px;font-size:12px;font-weight:600;color:var(--text2);border-radius:20px;cursor:pointer;background:var(--panel);border:1px solid var(--border);flex-shrink:0}.subtabs span.active{background:var(--yellow);color:#0b0e11;border-color:var(--yellow)}.list{padding:0 0 16px}.ob{background:var(--panel);border-radius:14px;margin:0 18px 14px;padding:14px 0;border:1px solid var(--border)}.ob-head{display:flex;padding:0 16px 12px;font-size:10px;color:var(--text3);text-transform:uppercase;font-weight:600}.ob-head span{flex:1}.ob-head span:nth-child(2){text-align:center}.ob-head span:nth-child(3){text-align:right}.ob-row{display:flex;padding:5px 16px;font-size:12px;font-family:monospace;position:relative;height:26px;align-items:center;cursor:pointer}.ob-row:active{background:var(--hover)}.ob-row .depth{position:absolute;right:0;top:0;bottom:0;background:var(--red-bg)}.ob-row.bid .depth{background:var(--green-bg);right:auto;left:0}.ob-row span{flex:1;position:relative;z-index:1}.ob-row .p{font-weight:600}.ob-row .a,.ob-row .t{color:var(--text2);text-align:right}.ob-row .v{text-align:center;color:var(--text2)}.ob-row.ask .p{color:var(--red)}.ob-row.bid .p{color:var(--green)}.spread{display:flex;justify-content:center;align-items:center;gap:10px;padding:10px 0;font-size:12px;font-weight:600;color:var(--text2);border-top:1px solid var(--border);border-bottom:1px solid var(--border);margin:8px 0}.spread .val{font-family:monospace;color:var(--text);font-size:15px}.pos{background:var(--panel);border-radius:14px;margin:0 18px 10px;padding:14px 16px;border:1px solid var(--border);display:grid;grid-template-columns:1fr auto;gap:10px}.pos .top{display:flex;align-items:center;gap:8px;grid-column:1/-1}.pos .sym{font-family:monospace;font-weight:700;font-size:15px}.pos .side{font-size:10px;font-weight:700;padding:3px 8px;border-radius:6px;text-transform:uppercase}.pos .side.long{background:var(--green-bg);color:var(--green)}.pos .side.short{background:var(--red-bg);color:var(--red)}.pos .sz{font-size:11px;color:var(--text3);font-family:monospace}.pos .meta{display:flex;gap:14px;font-size:11px;color:var(--text3);font-family:monospace;flex-wrap:wrap;grid-column:1/-1;margin-top:4px}.pos .meta b{color:var(--text2);font-weight:500}.pos .pnl{font-family:monospace;font-weight:700;font-size:17px;text-align:right}.pos .pnl-pct{font-size:11px;text-align:right;margin-top:2px}.closebtn{background:var(--red);color:#fff;padding:8px 16px;font-size:12px;font-weight:700;border-radius:8px;grid-column:2;justify-self:end;margin-top:8px}.oform{background:var(--panel);margin:0 18px 18px;border-radius:16px;padding:16px;border:1px solid var(--border)}.otoggle{display:flex;gap:6px;margin-bottom:14px;background:var(--panel2);padding:4px;border-radius:12px}.otoggle span{flex:1;text-align:center;font-size:12px;padding:9px 0;border-radius:9px;color:var(--text2);font-weight:600;cursor:pointer}.otoggle span.active{background:var(--hover);color:var(--text)}.row{display:flex;gap:8px;margin-bottom:10px}.row .cell{flex:1}.row label{font-size:10px;color:var(--text3);display:block;margin-bottom:5px;text-transform:uppercase;font-weight:600}.row input{padding:13px 14px;font-size:14px;font-family:monospace}.bigbtns{display:flex;gap:10px;margin-top:14px}.bigbtn{flex:1;padding:16px 0;font-weight:700;font-size:16px;border-radius:14px;display:flex;flex-direction:column;align-items:center;gap:3px}.bigbtn .lbl{font-size:11px;font-weight:600;opacity:.9}.bigbtn .px{font-family:monospace;font-size:15px;font-weight:700}.bigbtn.buy{background:var(--green);color:#0b0e11;box-shadow:0 6px 20px rgba(14,203,129,.25)}.bigbtn.sell{background:var(--red);color:#fff;box-shadow:0 6px 20px rgba(246,70,93,.25)}.btn-y{background:var(--yellow);color:#0b0e11;padding:14px;font-weight:700;font-size:14px;border-radius:12px;width:100%;margin-top:8px}.btn-r{background:var(--red);color:#fff;padding:14px;font-weight:700;font-size:14px;border-radius:12px;width:100%;margin-top:8px}.pt-card{background:var(--panel);border-radius:16px;margin:0 18px 14px;padding:18px;border:1px solid var(--border)}.pt-card h3{font-size:11px;color:var(--text3);text-transform:uppercase;font-weight:600;margin-bottom:14px}.stats4{display:grid;grid-template-columns:1fr 1fr;gap:12px}.stat-item{background:var(--panel2);padding:14px;border-radius:12px}.stat-item .lbl{font-size:10px;color:var(--text3);text-transform:uppercase;font-weight:600}.stat-item .val{font-family:monospace;font-weight:700;font-size:17px;margin-top:5px}table{width:100%;border-collapse:collapse;font-size:12px;font-family:monospace}th,td{text-align:left;padding:11px 6px;border-bottom:1px solid var(--border);white-space:nowrap}th{color:var(--text3);font-weight:600;font-size:10px;text-transform:uppercase;font-family:Inter,sans-serif}.pill.long{background:var(--green-bg);color:var(--green);padding:3px 8px;border-radius:8px;font-size:10px;font-weight:700}.pill.short{background:var(--red-bg);color:var(--red);padding:3px 8px;border-radius:8px;font-size:10px;font-weight:700}.xbtn{background:transparent;color:var(--red);padding:5px 10px;font-size:11px;border:1px solid var(--red);border-radius:6px;font-weight:600}.empty{padding:40px 24px;text-align:center;color:var(--text3);font-size:13px}.ptabs{display:flex;gap:6px;margin-bottom:16px;background:var(--panel2);padding:4px;border-radius:12px}.ptab{flex:1;text-align:center;padding:9px 0;font-size:12px;font-weight:600;color:var(--text2);border-radius:9px;cursor:pointer}.ptab.active{background:var(--hover);color:var(--text)}.acct{padding:0 18px 18px}.acct-item{padding:16px;background:var(--panel);display:flex;justify-content:space-between;align-items:center;font-size:14px;border-bottom:1px solid var(--border)}.acct-item:first-child{border-top-left-radius:16px;border-top-right-radius:16px}.acct-item:last-of-type{border-bottom:none;border-bottom-left-radius:16px;border-bottom-right-radius:16px}.acct-item .right{color:var(--text2);font-size:13px}.twofa{background:var(--panel);padding:18px;border-radius:16px;margin-bottom:14px;font-size:13px;line-height:1.6;border:1px solid var(--border)}.nav{background:var(--panel);border-top:1px solid var(--border);display:flex;flex-shrink:0;min-height:64px;padding-bottom:env(safe-area-inset-bottom,0);z-index:100}.nav button{flex:1;background:transparent;color:var(--text3);padding:10px 0 8px;font-size:10px;font-weight:600;display:flex;flex-direction:column;align-items:center;gap:4px;border-radius:0;border:none;text-transform:uppercase}.nav button.active{color:var(--yellow)}.nav .ic{font-size:22px;line-height:1;filter:grayscale(.4)}.nav button.active .ic{filter:none}.toasts{position:fixed;top:70px;left:18px;right:18px;z-index:1000;display:flex;flex-direction:column;gap:8px;pointer-events:none;align-items:center}.toast{background:var(--panel2);border-left:3px solid var(--yellow);padding:14px 18px;border-radius:10px;font-size:13px;font-weight:500;box-shadow:0 10px 30px rgba(0,0,0,.6);animation:sl .3s;max-width:400px}.toast.ok{border-left-color:var(--green)}.toast.err{border-left-color:var(--red)}@keyframes sl{from{transform:translateY(-20px);opacity:0}to{transform:none;opacity:1}}@keyframes fu{0%{background:var(--green-bg)}100%{background:transparent}}@keyframes fd{0%{background:var(--red-bg)}100%{background:transparent}}.fu{animation:fu .5s}.fd{animation:fd .5s}</style></head><body>

<header class="topbar">
  <div class="brand">⚡ TradeHub</div>
  <div class="conn" style="display:none"><span class="dot"></span><span id="connTxt">Live</span></div>
  <div class="spacer"></div>
  <div class="acct-mini"><b id="hEquity">—</b><span id="hUser"></span></div>
</header>

<div class="pages">
  <div class="page active" id="page-markets">
    <div class="cat-tabs" id="catTabs">
      <div class="cat-tab active" data-cat="crypto">Crypto</div>
      <div class="cat-tab" data-cat="forex">Forex</div>
      <div class="cat-tab" data-cat="commodities">Commodities</div>
      <div class="cat-tab" data-cat="stocks">Stocks</div>
      <div class="cat-tab" data-cat="indices">Indices</div>
    </div>
    <div id="instrList"></div>
  </div>

  <div class="page" id="page-trade">
    <div class="trade-hd">
      <div class="symline"><span class="symbig" id="symBig">BTC/USD</span><span class="badge" id="symLev">1:10</span></div>
      <div class="ba-row">
        <div class="ba sell"><div class="lbl">Sell · Bid</div><div class="val" id="bigBid">—</div></div>
        <div class="ba buy"><div class="lbl">Buy · Ask</div><div class="val" id="bigAsk">—</div></div>
      </div>
    </div>
    <div class="chartwrap">
      <div class="tf" id="tfBar">
        <span data-tf="1m">1m</span><span data-tf="5m">5m</span><span data-tf="15m">15m</span>
        <span data-tf="1h">1h</span><span class="on" data-tf="4h">4h</span><span data-tf="1d">1d</span>
      </div>
      <div class="candles" id="candles"><div class="paxis" id="paxis"></div></div>
    </div>
  <div style="padding:14px 18px 0" id="signalPanel">
  <div style="background:var(--panel);border:1px solid var(--border);border-radius:14px;padding:14px;display:flex;justify-content:space-between;align-items:center">
    <div>
      <div style="font-size:10px;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;font-weight:600">Signal</div>
      <div id="sigOverall" style="font-family:monospace;font-weight:800;font-size:18px;margin-top:2px;color:var(--text2)">—</div>
    </div>
    <div style="text-align:right">
      <div style="font-size:10px;color:var(--text3);text-transform:uppercase;letter-spacing:.5px;font-weight:600">Confidence</div>
      <div id="sigScore" style="font-family:monospace;font-weight:700;font-size:14px;color:var(--text2)">—</div>
    </div>
  </div>
  <div id="sigParts" style="margin-top:8px;display:flex;gap:6px;flex-wrap:wrap"></div>
</div>  <div class="subtabs" id="subTabs">
      <span class="active" data-sub="book">Order Book</span>
      <span data-sub="pos">Positions</span>
      <span data-sub="pend">Pending</span>
      <span data-sub="hist">History</span>
    </div>
    <div class="list" id="subContent"></div>
    <form class="oform" id="oform" onsubmit="return false">
      <div class="otoggle" id="otoggle">
        <span class="active" data-ot="market">Market</span>
        <span data-ot="pending">Pending</span>
      </div>
      <div id="pendingTypeRow" style="display:none;margin-bottom:10px">
        <div class="row"><div class="cell"><label>Order type</label>
          <select id="pType">
            <option value="buy_limit">Buy Limit (below)</option>
            <option value="sell_limit">Sell Limit (above)</option>
            <option value="buy_stop">Buy Stop (above)</option>
            <option value="sell_stop">Sell Stop (below)</option>
          </select>
        </div></div>
      </div>
      <div class="row">
        <div class="cell"><label>Size</label><input id="fSize" type="number" step="any" placeholder="0.01"></div>
        <div class="cell" id="priceCell" style="display:none"><label>Trigger Price</label><input id="fTrigger" type="number" step="any"></div>
      </div>
      <div class="row">
        <div class="cell"><label>Stop Loss</label><input id="fSL" type="number" step="any" placeholder="optional"></div>
        <div class="cell"><label>Take Profit</label><input id="fTP" type="number" step="any" placeholder="optional"></div>
      </div>
      <div class="bigbtns" id="bigBtns">
        <button type="button" class="bigbtn buy" id="btnBuy"><span class="lbl">BUY</span><span class="px" id="btnBuyPx">—</span></button>
        <button type="button" class="bigbtn sell" id="btnSell"><span class="lbl">SELL</span><span class="px" id="btnSellPx">—</span></button>
      </div>
      <button type="button" id="btnPlacePending" class="btn-y" style="display:none;margin-top:10px;padding:16px">Place Pending Order</button>
    </form>
  </div>

  <div class="page" id="page-portfolio">
    <div style="padding:16px 18px 8px"></div>
    <div class="pt-card">
      <h3>Account Summary</h3>
      <div class="stats4">
        <div class="stat-item"><div class="lbl">Balance</div><div class="val" id="pBalance">—</div></div>
        <div class="stat-item"><div class="lbl">Equity</div><div class="val" id="pEquity">—</div></div>
        <div class="stat-item"><div class="lbl">Unrealized</div><div class="val" id="pUnreal">—</div></div>
        <div class="stat-item"><div class="lbl">Free Margin</div><div class="val" id="pFree">—</div></div>
        <div class="stat-item"><div class="lbl">Used Margin</div><div class="val" id="pUsed">—</div></div>
        <div class="stat-item"><div class="lbl">Margin Level</div><div class="val" id="pML">—</div></div>
      </div>
    </div>
    <div class="pt-card">
      <div class="ptabs" id="portTabs">
        <div class="ptab active" data-ptab="pos">Open</div>
        <div class="ptab" data-ptab="hist">Closed</div>
      </div>
      <div id="portContent"><div class="empty">Loading…</div></div>
    </div>
  </div>

  <div class="page" id="page-account">
    <div style="padding:16px 18px 8px"></div>
    <div class="acct">
      <div class="acct-item"><b>Username</b><span class="right" id="acctUser">—</span></div>
      <div class="acct-item"><b>Email</b><span class="right" id="acctEmail">—</span></div>
      <div class="acct-item"><b>Role</b><span class="right" id="acctRole">—</span></div>
      <div class="acct-item" id="adminRow" style="display:none"><b>Admin Panel</b><span class="right">→</span></div>
    </div>
    <div class="acct" style="margin-top:14px">
      <div class="twofa" id="subBox">
        <b>Subscription</b>
        <div id="subStatus" style="margin-top:6px;color:var(--text2)">Loading…</div>
        <div id="subActions" style="margin-top:10px"></div>
      </div>
    </div>
    <div class="acct" style="margin-top:14px">
      <div class="twofa">
        <b>Two-Factor Authentication</b>
        <div id="twofaStatus" style="margin-top:6px;color:var(--text2)">Loading…</div>
        <div id="twofaActions" style="margin-top:10px"></div>
      </div>
      <button class="btn-r" id="logoutBtn" style="margin-top:8px">Logout</button>
    </div>
  </div>
</div>

<nav class="nav" id="nav">
  <button class="active" data-page="markets"><span class="ic">📊</span>Markets</button>
  <button data-page="trade"><span class="ic">📈</span>Trade</button>
  <button data-page="portfolio"><span class="ic">💼</span>Portfolio</button>
  <button data-page="account"><span class="ic">👤</span>Account</button>
</nav>

<div class="toasts" id="toasts"></div>

<script>
function $(s){return document.querySelector(s)}
function $$(s){return document.querySelectorAll(s)}
function fmt(n,d){if(n==null||isNaN(n))return'—';return Number(n).toLocaleString('en-US',{minimumFractionDigits:d==null?2:d,maximumFractionDigits:d==null?2:d})}
function fp(p){if(p==null||isNaN(p))return'—';const a=Math.abs(p);if(a>=1000)return fmt(p,2);if(a>=1)return p.toFixed(2);if(a>=0.01)return p.toFixed(4);return p.toFixed(6)}
function money(n){if(n==null||isNaN(n))return'—';const s=n>=0?'+':'';return s+'$'+fmt(Math.abs(n),2)}

var state={instruments:[],prices:{},bids:{},asks:{},user:null,account:null,positions:[],pending:[],history:[],symbol:'BTCUSDT',category:'crypto',page:'markets',subtab:'book',portTab:'pos',orderType:'market',timeframe:'4h',chartData:[],orderBook:{bids:[],asks:[]}};

function toast(m,t){var el=document.createElement('div');el.className='toast '+(t||'');el.textContent=m;$('#toasts').appendChild(el);setTimeout(function(){el.style.transition='opacity .3s';el.style.opacity='0';setTimeout(function(){el.remove()},300)},3200)}

var ws;
function connectWS(){var proto=location.protocol==='https:'?'wss://':'ws://';ws=new WebSocket(proto+location.host+'/ws');ws.onopen=function(){$('#connTxt').textContent='Live'};ws.onmessage=function(e){try{var m=JSON.parse(e.data);if(m.symbol&&m.mid!=null)onTick(m.symbol,m.mid)}catch(err){}};ws.onclose=function(){$('#connTxt').textContent='Offline';setTimeout(connectWS,3000)};ws.onerror=function(){$('#connTxt').textContent='Offline'}}

function onTick(sym,mid){
  state.prices[sym]=mid;
  var inst=state.instruments.find(function(x){return x.symbol===sym});
  if(inst){var sp=inst.ask-inst.bid;state.bids[sym]=mid-sp/2;state.asks[sym]=mid+sp/2}
  var row=document.querySelector('.instr[data-sym="'+sym+'"]');
  if(row){
    var i2=state.instruments.find(function(x){return x.symbol===sym});
    var pEl=row.querySelector('.price');var pillEl=row.querySelector('.pill');
    if(pEl&&pillEl&&i2){var ref=i2.start||mid;var chg=((mid-ref)/ref)*100;pEl.textContent=fp(mid);pillEl.className='pill '+(chg>=0?'up':'down');pillEl.textContent=(chg>=0?'↑ +':'↓ ')+chg.toFixed(2)+'%'}
  }
  if(sym===state.symbol){updateBigPrices();updateLastCandle(mid);if(state.subtab==='book')renderOrderBook()}
  if(state.page==='trade'&&state.subtab==='pos')refreshPositionsUI();
}

$$('.nav button').forEach(function(b){b.onclick=function(){setPage(b.dataset.page)}});
function setPage(p){state.page=p;$$('.page').forEach(function(el){el.classList.remove('active')});$('#page-'+p).classList.add('active');$$('.nav button').forEach(function(b){b.classList.toggle('active',b.dataset.page===p)});if(p==='portfolio')renderPortfolio();if(p==='trade')refreshSubContent()}

$$('.cat-tab').forEach(function(t){t.onclick=function(){state.category=t.dataset.cat;$$('.cat-tab').forEach(function(x){x.classList.toggle('active',x.dataset.cat===state.category)});renderMarkets()}});

var CAT_ICONS={crypto:'₿',forex:'💱',commodities:'🥇',stocks:'📈',indices:'📊'};

function renderMarkets(){
  var list=state.instruments.filter(function(x){return x.cat===state.category});
  var el=$('#instrList');
  if(!list.length){el.innerHTML='<div class="empty">No instruments.</div>';return}
  el.innerHTML=list.map(function(i){
    var mid=state.prices[i.symbol]||i.start||((i.bid+i.ask)/2);
    var ref=i.start||mid;var chg=((mid-ref)/ref)*100;var upCls=chg>=0?'up':'down';
    var icon=CAT_ICONS[i.cat]||'•';
    return '<div class="instr" data-sym="'+i.symbol+'">'+
      '<div class="left"><div class="icon">'+icon+'</div>'+
      '<div><div class="name">'+i.base+'/'+i.quote+'</div><div class="sub">'+i.cat+' · 1:'+i.leverage+'</div></div></div>'+
      '<div class="right"><div class="price">'+fp(mid)+'</div><div class="pill '+upCls+'">'+(chg>=0?'↑ +':'↓ ')+chg.toFixed(2)+'%</div></div>'+
    '</div>';
  }).join('');
  $$('.instr').forEach(function(el){el.onclick=function(){switchSymbol(el.dataset.sym);setPage('trade')}});
}

function switchSymbol(sym){
  state.symbol=sym;
  var i=state.instruments.find(function(x){return x.symbol===sym});
  if(!i)return;
  $('#symBig').textContent=i.base+'/'+i.quote;
  $('#symLev').textContent='1:'+i.leverage;
  $('#fSize').placeholder=i.base==='BTC'?'0.01':i.cat==='forex'?'1000':'1';
  updateBigPrices();generateOrderBook();loadKlines();refreshSubContent();
}

function updateBigPrices(){
  var i=state.instruments.find(function(x){return x.symbol===state.symbol});
  if(!i)return;
  var mid=state.prices[i.symbol]||i.start||((i.bid+i.ask)/2);
  var sp=i.ask-i.bid;var bid=mid-sp/2,ask=mid+sp/2;
  $('#bigBid').textContent=fp(bid);$('#bigAsk').textContent=fp(ask);
  $('#btnBuyPx').textContent=fp(ask);$('#btnSellPx').textContent=fp(bid);
}

async function loadKlines(){
  try{var r=await fetch('/api/klines/'+state.symbol+'?interval='+state.timeframe+'&limit=60');var j=await r.json();if(!j.candles||!j.candles.length)return;state.chartData=j.candles;renderChart()}catch(e){}
}
$$('#tfBar span').forEach(function(s){s.onclick=function(){$$('#tfBar span').forEach(function(x){x.classList.remove('on')});s.classList.add('on');state.timeframe=s.dataset.tf;loadKlines()}});

function renderChart(){
  var box=$('#candles'),paxis=$('#paxis');
  Array.from(box.querySelectorAll('.candle')).forEach(function(el){el.remove()});
  var data=state.chartData;if(!data||!data.length)return;
  var mn=Infinity,mx=-Infinity;
  data.forEach(function(c){mn=Math.min(mn,c.l);mx=Math.max(mx,c.h)});
  var pad=(mx-mn)*0.1||1;mn-=pad;mx+=pad;var range=mx-mn;
  paxis.innerHTML='';
  for(var k=5;k>=0;k--){var sp=document.createElement('span');sp.textContent=fp(mn+range*k/5);paxis.appendChild(sp)}
  data.forEach(function(c){
    var el=document.createElement('div');el.className='candle '+(c.c>=c.o?'green':'red');
    var bH=Math.max(1,(Math.abs(c.c-c.o)/range)*100);var bB=((Math.min(c.o,c.c)-mn)/range)*100;
    var wT=((c.h-mn)/range)*100;var wB=((c.l-mn)/range)*100;
    el.innerHTML='<div class="wick" style="height:'+(wT-wB)+'%;bottom:'+wB+'%"></div><div class="body" style="height:'+bH+'%;margin-bottom:'+bB+'%"></div>';
    box.appendChild(el);
  });
}

function updateLastCandle(mid){if(!state.chartData||!state.chartData.length)return;var last=state.chartData[state.chartData.length-1];last.c=mid;if(mid>last.h)last.h=mid;if(mid<last.l)last.l=mid;renderChart()}

function generateOrderBook(){
  var mid=state.prices[state.symbol]||0;if(!mid)return;
  var step=Math.max(mid*0.0004,0.0001);
  var asks=[],bids=[],cumA=0,cumB=0;
  for(var i=1;i<=6;i++){
    var aa=0.05+Math.random()*2.5;var ab=0.05+Math.random()*2.5;
    cumA+=aa;cumB+=ab;
    asks.push({p:mid+step*i,a:aa,c:cumA});bids.push({p:mid-step*i,a:ab,c:cumB});
  }
  state.orderBook={asks:asks.reverse(),bids:bids,maxA:cumA,maxB:cumB,step:step,mid:mid};
}

function renderOrderBook(){
  var ob=state.orderBook;if(!ob.asks||!ob.asks.length)return;
  var html='<div class="ob"><div class="ob-head"><span>Price (USD)</span><span>Volume</span><span>Total</span></div>';
  ob.asks.forEach(function(r){var w=Math.min(100,(r.c/ob.maxA)*100);html+='<div class="ob-row ask" data-p="'+r.p+'"><div class="depth" style="width:'+w+'%"></div><span class="p">'+fp(r.p)+'</span><span class="v">'+r.a.toFixed(3)+'</span><span class="t">'+fmt(r.c,0)+'</span></div>'});
  html+='<div class="spread">— spread — <span class="val">'+fp(ob.mid)+'</span></div>';
  ob.bids.forEach(function(r){var w=Math.min(100,(r.c/ob.maxB)*100);html+='<div class="ob-row bid" data-p="'+r.p+'"><div class="depth" style="width:'+w+'%"></div><span class="p">'+fp(r.p)+'</span><span class="v">'+r.a.toFixed(3)+'</span><span class="t">'+fmt(r.c,0)+'</span></div>'});
  html+='</div>';
  $('#subContent').innerHTML=html;
}

$$('#subTabs span').forEach(function(s){s.onclick=function(){state.subtab=s.dataset.sub;$$('#subTabs span').forEach(function(x){x.classList.toggle('active',x.dataset.sub===state.subtab)});refreshSubContent()}});

function refreshSubContent(){
  if(state.subtab==='book'){renderOrderBook();return}
  var el=$('#subContent');
  if(state.subtab==='pos'){
    if(!state.positions.length){el.innerHTML='<div class="empty">No open positions</div>';return}
    el.innerHTML=state.positions.map(function(p){
      var i=state.instruments.find(function(x){return x.symbol===p.symbol})||{};
      var mid=state.prices[p.symbol]||p.entry;var sp=(i.ask||mid)-(i.bid||mid);
      var cp=p.side==='long'?(i.bid||mid-sp/2):(i.ask||mid+sp/2);
      var pnl=(cp-p.entry)*p.size*(p.side==='long'?1:-1);
      var pnlPct=p.entry*p.size>0?(pnl/(p.entry*p.size))*100:0;
      return '<div class="pos"><div class="top"><span class="sym">'+p.symbol+'</span><span class="side '+p.side+'">'+p.side.toUpperCase()+'</span><span class="sz">'+p.size+'</span></div>'+
        '<div class="meta"><span>Entry <b>'+fp(p.entry)+'</b></span><span>Now <b>'+fp(cp)+'</b></span>'+(p.sl?'<span>SL <b>'+fp(p.sl)+'</b></span>':'')+(p.tp?'<span>TP <b>'+fp(p.tp)+'</b></span>':'')+'</div>'+
        '<div style="text-align:right"><div class="pnl '+(pnl>=0?'up':'down')+'">'+money(pnl)+'</div><div class="pnl-pct '+(pnlPct>=0?'up':'down')+'">'+(pnlPct>=0?'+':'')+pnlPct.toFixed(2)+'%</div></div>'+
        '<button class="closebtn" onclick="closePos('+p.id+')">Close</button></div>';
    }).join('');
  }else if(state.subtab==='pend'){
    if(!state.pending.length){el.innerHTML='<div class="empty">No pending orders</div>';return}
    el.innerHTML='<div style="padding:0 18px"><table><thead><tr><th>Type</th><th>Pair</th><th>Trigger</th><th>Size</th><th></th></tr></thead><tbody>'+state.pending.map(function(o){return '<tr><td>'+o.type.replace('_',' ')+'</td><td>'+o.symbol+'</td><td>'+fp(o.price)+'</td><td>'+o.size+'</td><td><button class="xbtn" onclick="cancelPending('+o.id+')">×</button></td></tr>'}).join('')+'</tbody></table></div>';
  }else{
    if(!state.history.length){el.innerHTML='<div class="empty">No closed trades</div>';return}
    el.innerHTML='<div style="padding:0 18px"><table><thead><tr><th>Pair</th><th>Side</th><th>Size</th><th>P&L</th></tr></thead><tbody>'+state.history.slice(0,30).map(function(h){return '<tr><td>'+h.symbol+'</td><td><span class="pill '+h.side+'">'+h.side.toUpperCase()+'</span></td><td>'+h.size+'</td><td class="'+(h.pnl>=0?'up':'down')+'">'+money(h.pnl)+'</td></tr>'}).join('')+'</tbody></table></div>';
  }
}

function refreshPositionsUI(){$$('.pos').forEach(function(el,idx){var p=state.positions[idx];if(!p)return;var i=state.instruments.find(function(x){return x.symbol===p.symbol})||{};var mid=state.prices[p.symbol]||p.entry;var sp=(i.ask||mid)-(i.bid||mid);var cp=p.side==='long'?(i.bid||mid-sp/2):(i.ask||mid+sp/2);var pnl=(cp-p.entry)*p.size*(p.side==='long'?1:-1);var pnlPct=p.entry*p.size>0?(pnl/(p.entry*p.size))*100:0;var pnlEl=el.querySelector('.pnl');var pctEl=el.querySelector('.pnl-pct');if(pnlEl){pnlEl.textContent=money(pnl);pnlEl.className='pnl '+(pnl>=0?'up':'down')}if(pctEl){pctEl.textContent=(pnlPct>=0?'+':'')+pnlPct.toFixed(2)+'%';pctEl.className='pnl-pct '+(pnlPct>=0?'up':'down')}})}

$$('#otoggle span').forEach(function(s){s.onclick=function(){state.orderType=s.dataset.ot;$$('#otoggle span').forEach(function(x){x.classList.toggle('active',x.dataset.ot===state.orderType)});if(state.orderType==='pending'){$('#pendingTypeRow').style.display='block';$('#priceCell').style.display='block';$('#btnPlacePending').style.display='block';$('#bigBtns').style.display='none'}else{$('#pendingTypeRow').style.display='none';$('#priceCell').style.display='none';$('#btnPlacePending').style.display='none';$('#bigBtns').style.display='flex'}}});

$('#btnBuy').onclick=function(){placeMarket('long')};
$('#btnSell').onclick=function(){placeMarket('short')};
$('#btnPlacePending').onclick=placePending;

async function placeMarket(side){
  var size=parseFloat($('#fSize').value);
  if(!size||size<=0){toast('Enter a size','err');return}
  var sl=parseFloat($('#fSL').value)||null;
  var tp=parseFloat($('#fTP').value)||null;
  var r=await fetch('/api/positions',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol:state.symbol,side:side,size:size,sl:sl,tp:tp})});
  var j=await r.json();
  if(j.ok){toast('Opened '+side.toUpperCase()+' '+size+' @ '+fp(j.entry),'ok');$('#fSize').value='';$('#fSL').value='';$('#fTP').value='';await refreshAll()}
  else toast(j.error||'Failed','err');
}

async function placePending(){
  var size=parseFloat($('#fSize').value);var price=parseFloat($('#fTrigger').value);var type=$('#pType').value;
  var sl=parseFloat($('#fSL').value)||null;var tp=parseFloat($('#fTP').value)||null;
  if(!size||!price){toast('Enter size and trigger price','err');return}
  var r=await fetch('/api/pending',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({symbol:state.symbol,type:type,price:price,size:size,sl:sl,tp:tp})});
  var j=await r.json();
  if(j.ok){toast('Pending placed','ok');$('#fSize').value='';$('#fTrigger').value='';$('#fSL').value='';$('#fTP').value='';await refreshAll()}
  else toast(j.error||'Failed','err');
}

window.closePos=async function(id){var r=await fetch('/api/positions/'+id+'/close',{method:'POST'});var j=await r.json();if(j.ok){toast('Closed · P&L '+money(j.pnl),j.pnl>=0?'ok':'');await refreshAll()}else toast(j.error||'Failed','err')};
window.cancelPending=async function(id){await fetch('/api/pending/'+id,{method:'DELETE'});toast('Cancelled','');await refreshAll()};

$$('#portTabs .ptab').forEach(function(t){t.onclick=function(){state.portTab=t.dataset.ptab;$$('#portTabs .ptab').forEach(function(x){x.classList.toggle('active',x.dataset.ptab===state.portTab)});renderPortfolio()}});

function renderPortfolio(){
  var el=$('#portContent');
  if(state.portTab==='pos'){
    if(!state.positions.length){el.innerHTML='<div class="empty">No open positions</div>';return}
    el.innerHTML='<table><thead><tr><th>Pair</th><th>Side</th><th>Size</th><th>Entry</th><th>P&L</th></tr></thead><tbody>'+state.positions.map(function(p){var i=state.instruments.find(function(x){return x.symbol===p.symbol})||{};var mid=state.prices[p.symbol]||p.entry;var cp=p.side==='long'?(i.bid||mid):(i.ask||mid);var pnl=(cp-p.entry)*p.size*(p.side==='long'?1:-1);return '<tr><td>'+p.symbol+'</td><td><span class="pill '+p.side+'">'+p.side.toUpperCase()+'</span></td><td>'+p.size+'</td><td>'+fp(p.entry)+'</td><td class="'+(pnl>=0?'up':'down')+'">'+money(pnl)+'</td></tr>'}).join('')+'</tbody></table>';
  }else{
    if(!state.history.length){el.innerHTML='<div class="empty">No closed trades</div>';return}
    el.innerHTML='<table><thead><tr><th>Pair</th><th>Side</th><th>Size</th><th>P&L</th></tr></thead><tbody>'+state.history.slice(0,40).map(function(h){return '<tr><td>'+h.symbol+'</td><td><span class="pill '+h.side+'">'+h.side.toUpperCase()+'</span></td><td>'+h.size+'</td><td class="'+(h.pnl>=0?'up':'down')+'">'+money(h.pnl)+'</td></tr>'}).join('')+'</tbody></table>';
  }
}

$('#adminRow').onclick=function(){location.href='/admin'};
$('#logoutBtn').onclick=async function(){await fetch('/api/auth/logout',{method:'POST'});location.href='/login'};

function render2FA(){
  var st=$('#twofaStatus'),ac=$('#twofaActions');
  if(state.user.totpEnabled){st.innerHTML='<b class="up">Enabled</b>';ac.innerHTML='<button class="btn-r" onclick="disable2FA()">Disable</button>'}
  else{st.innerHTML='<span class="muted">Disabled</span>';if(state.user.isAdmin)ac.innerHTML='<button class="btn-y" onclick="setup2FA()">Setup 2FA</button>';else ac.innerHTML='<span class="muted" style="font-size:11px">Only admins can enable</span>'}
}

window.setup2FA=async function(){
  var r=await fetch('/api/auth/2fa/setup',{method:'POST'});var j=await r.json();
  if(!j.ok){toast(j.error||'Failed','err');return}
  var code=prompt('Add to Google Authenticator (setup key):\\n\\n'+j.secret+'\\n\\nEnter 6-digit code:');
  if(!code)return;
  var r2=await fetch('/api/auth/2fa/enable',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({code:code})});
  var j2=await r2.json();
  if(j2.ok){toast('2FA enabled','ok');state.user.totpEnabled=true;render2FA()}else toast(j2.error||'Invalid','err');
};
window.disable2FA=async function(){if(!confirm('Disable 2FA?'))return;await fetch('/api/auth/2fa/disable',{method:'POST'});state.user.totpEnabled=false;render2FA();toast('2FA disabled','')};

async function loadSubscription(){
  var r=await fetch('/api/subscription');if(!r.ok)return;
  var j=await r.json();
  var el=$('#subStatus'),ac=$('#subActions');
  if(j.pendingRequest){
    el.innerHTML='<b class="muted">Pending approval</b> — $'+fmt(j.pendingRequest.balance_limit,0)+' balance · KES '+fmt(j.pendingRequest.amount_kes,0);
    ac.innerHTML='';return;
  }
  if(j.isPro){
    var exp=new Date(j.subscription.expires_at*1000);
    el.innerHTML='<b class="up">Pro Active</b> — up to $'+fmt(j.subscription.balance_limit,0)+' · expires '+exp.toLocaleDateString();
    ac.innerHTML='<button class="btn-y" onclick="showPlans()">Change / Extend</button>';
  }else{
    el.innerHTML='<span class="muted">Free tier</span> — up to $'+fmt(j.freeBalance,0)+' paper balance';
    ac.innerHTML='<button class="btn-y" onclick="showPlans()">Upgrade</button>';
  }
}

async function showPlans(){
  var r=await fetch('/api/subscription');var j=await r.json();
  var bal=prompt('Choose your trading balance:\\n\\nFree tier: up to $'+j.freeBalance.toLocaleString()+'\\nMin paid: $'+j.minBalance.toLocaleString()+'\\nMax: $'+j.maxBalance.toLocaleString()+'\\n\\nRate: KES '+j.rate+' per $1,000 per 30 days\\n\\nEnter desired balance (USD):','100000');
  if(!bal)return;
  var q=await fetch('/api/subscription/quote?balance='+encodeURIComponent(bal)).then(function(r){return r.json()});
  if(q.isFree){alert('Balances up to $'+j.freeBalance.toLocaleString()+' are on the free tier — no payment needed.');return}
  alert('Balance: $'+q.balance.toLocaleString()+'\\nPrice: KES '+q.price_kes+' for '+q.days+' days\\n\\nPay to:\\nPaybill: '+j.paybill+'\\nAccount: '+j.paybillAccount+'\\n\\nAfter paying, tap OK to enter your M-Pesa confirmation code.');
  var ref=prompt('Enter M-Pesa confirmation code (e.g. QGH7XYZ123):');
  if(!ref)return;
  var s=await fetch('/api/subscription/request',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({balance_limit:q.balance,reference:ref,method:'mpesa'})});
  var sj=await s.json();
  if(sj.ok){toast('Payment submitted — awaiting admin approval','ok');loadSubscription()}
  else toast(sj.error||'Failed','err');
}

async function refreshAccount(){
  var r=await fetch('/api/account');if(!r.ok)return;
  state.account=await r.json();
  var a=state.account;
  $('#pBalance').textContent='$'+fmt(a.balance);
  $('#pEquity').textContent='$'+fmt(a.equity);
  $('#pUnreal').textContent=money(a.unrealized);
  $('#pUnreal').className='val '+(a.unrealized>=0?'up':'down');
  $('#pFree').textContent='$'+fmt(a.freeMargin);
  $('#pUsed').textContent='$'+fmt(a.usedMargin);
  $('#pML').textContent=a.marginLevel!=null?a.marginLevel.toFixed(1)+'%':'—';
  $('#hEquity').textContent='$'+fmt(a.equity);
}

async function refreshPositions(){var r=await fetch('/api/positions');if(!r.ok)return;var j=await r.json();state.positions=j.positions;if(state.subtab==='pos'&&state.page==='trade')refreshSubContent();if(state.portTab==='pos'&&state.page==='portfolio')renderPortfolio()}
async function refreshPending(){var r=await fetch('/api/pending');if(!r.ok)return;var j=await r.json();state.pending=j.pending;if(state.subtab==='pend'&&state.page==='trade')refreshSubContent()}
async function refreshHistory(){var r=await fetch('/api/history');if(!r.ok)return;var j=await r.json();state.history=j.history;if(state.subtab==='hist'&&state.page==='trade')refreshSubContent();if(state.portTab==='hist'&&state.page==='portfolio')renderPortfolio()}
async function refreshAll(){await Promise.all([refreshAccount(),refreshPositions(),refreshPending(),refreshHistory()])}

async function init(){
  var me=await fetch('/api/auth/me');if(!me.ok){location.href='/login';return}
  var m=await me.json();state.user=m.user;
  $('#hUser').textContent=m.user.username;
  $('#acctUser').textContent=m.user.username;
  $('#acctEmail').textContent=m.user.email;
  $('#acctRole').textContent=m.user.isAdmin?'Admin':'User';
  if(m.user.isAdmin)$('#adminRow').style.display='flex';
  render2FA();loadSubscription();
  var r=await fetch('/api/instruments');var j=await r.json();
  state.instruments=j.instruments;
  j.instruments.forEach(function(i){state.prices[i.symbol]=i.mid;state.bids[i.symbol]=i.bid;state.asks[i.symbol]=i.ask;i.start=i.mid});
  renderMarkets();switchSymbol('BTCUSDT');
  await refreshAll();connectWS();
}

setInterval(async function(){await refreshAccount();await refreshPositions();if(state.page==='trade')await refreshPending();if(state.page==='trade'&&state.subtab==='book')generateOrderBook()},4000);
setInterval(function(){if(state.page==='trade'&&state.subtab==='book')renderOrderBook()},2000);

init();
<\/script></body></html>`;

const ADMIN_HTML=`<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>TradeHub Admin</title><style>${BASE_CSS}body{padding-bottom:40px}.top{background:var(--panel);padding:14px 18px;display:flex;align-items:center;gap:16px;border-bottom:1px solid var(--border);position:sticky;top:0;z-index:10}.brand{color:var(--yellow);font-weight:800;font-size:16px}.top a{color:var(--yellow);font-size:13px;text-decoration:none}.wrap{max-width:1300px;margin:0 auto;padding:16px;display:flex;flex-direction:column;gap:16px}.panel{background:var(--panel);border:1px solid var(--border);border-radius:14px;padding:18px;overflow-x:auto}.panel h2{font-size:14px;color:var(--yellow);margin-bottom:14px;font-weight:700}table{width:100%;border-collapse:collapse;font-size:12px;min-width:520px;font-family:monospace}th,td{text-align:left;padding:10px 8px;border-bottom:1px solid var(--border);white-space:nowrap}th{color:var(--text3);font-weight:600;font-size:10px;text-transform:uppercase;font-family:Inter,sans-serif}td button{background:var(--blue);color:#fff;padding:5px 10px;font-size:11px;margin-right:4px;border:none;border-radius:6px;cursor:pointer;font-family:inherit;font-weight:600}td button.danger{background:var(--red)}td button.green{background:var(--green);color:#0b0e11}td button.yellow{background:var(--yellow);color:#0b0e11}.empty{padding:40px;text-align:center;color:var(--text3);font-size:13px}.pill{padding:3px 8px;border-radius:8px;font-size:10px;font-weight:700;font-family:Inter,sans-serif}.pill.pending{background:rgba(240,185,11,.12);color:var(--yellow)}.pill.approved{background:var(--green-bg);color:var(--green)}.pill.rejected{background:var(--red-bg);color:var(--red)}</style></head><body><div class="top"><div class="brand">⚡ TradeHub Admin</div><div class="spacer"></div><a href="/">← Terminal</a></div><div class="wrap" id="wrap"><div class="empty">Loading…</div></div><script>function $(s){return document.querySelector(s)}
async function load(){
  var me=await fetch('/api/auth/me');
  if(!me.ok){location.href='/login';return}
  var md=await me.json();
  if(!md.user.isAdmin){$('#wrap').innerHTML='<div class="empty">Admin required</div>';return}
  var u=await fetch('/api/admin/users').then(function(r){return r.json()});
  var pr=await fetch('/api/admin/payment-requests').then(function(r){return r.json()});
  var p=await fetch('/api/admin/positions').then(function(r){return r.json()});

  var payHtml=pr.requests.length?pr.requests.map(function(r){
    return '<tr>'+
      '<td>'+r.id+'</td>'+
      '<td>'+r.username+'</td>'+
      '<td>$'+Number(r.balance_limit).toLocaleString()+'</td>'+
      '<td>KES '+Number(r.amount_kes).toLocaleString()+'</td>'+
      '<td>'+r.reference+'</td>'+
      '<td>'+new Date(r.created_at*1000).toLocaleString()+'</td>'+
      '<td><span class="pill '+r.status+'">'+r.status+'</span></td>'+
      '<td>'+(r.status==='pending'?'<button class="green" onclick="approvePay('+r.id+')">Approve</button><button class="danger" onclick="rejectPay('+r.id+')">Reject</button>':'—')+'</td>'+
    '</tr>';
  }).join(''):'<tr><td colspan="8" class="empty">No payment requests yet</td></tr>';

  $('#wrap').innerHTML=
    '<div class="panel"><h2>⚡ Subscription Payments</h2>'+
      '<table><thead><tr><th>ID</th><th>User</th><th>Balance</th><th>Amount</th><th>Ref</th><th>Requested</th><th>Status</th><th>Actions</th></tr></thead><tbody>'+payHtml+'</tbody></table>'+
    '</div>'+
    '<div class="panel"><h2>Users ('+u.users.length+')</h2><table><thead><tr><th>ID</th><th>Username</th><th>Email</th><th>Admin</th><th>Balance</th><th>Actions</th></tr></thead><tbody>'+
      u.users.map(function(x){return '<tr><td>'+x.id+'</td><td>'+x.username+'</td><td>'+x.email+'</td><td>'+(x.is_admin?'✅':'—')+'</td><td>$'+Number(x.balance||0).toFixed(2)+'</td><td>'+(!x.is_admin?'<button onclick="promote('+x.id+')">Promote</button>':'<button class="danger" onclick="demote('+x.id+')">Demote</button>')+'<button class="yellow" onclick="credit('+x.id+')">Credit</button></td></tr>'}).join('')+
    '</tbody></table></div>'+
    '<div class="panel"><h2>All Positions ('+p.positions.length+')</h2><table><thead><tr><th>ID</th><th>User</th><th>Symbol</th><th>Side</th><th>Size</th><th>Entry</th><th>Status</th><th>P&L</th></tr></thead><tbody>'+
      p.positions.map(function(r){return '<tr><td>'+r.id+'</td><td>'+r.username+'</td><td>'+r.symbol+'</td><td>'+r.side+'</td><td>'+r.size+'</td><td>'+r.entry_price+'</td><td>'+r.status+'</td><td>'+(r.pnl!=null?Number(r.pnl).toFixed(2):'—')+'</td></tr>'}).join('')+
    '</tbody></table></div>';
}
window.promote=async function(id){await fetch('/api/admin/users/'+id+'/promote',{method:'POST'});load()};
window.demote=async function(id){if(!confirm('Demote?'))return;await fetch('/api/admin/users/'+id+'/demote',{method:'POST'});load()};
window.credit=async function(id){var amount=prompt('Manual credit amount (USD):','100');if(!amount)return;await fetch('/api/admin/users/'+id+'/credit',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({amount:Number(amount)})});load()};
window.approvePay=async function(id){if(!confirm('Confirm payment received?'))return;var note=prompt('Optional note:')||'';var r=await fetch('/api/admin/payment-requests/'+id+'/approve',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({note:note})});var j=await r.json();if(!j.ok)alert(j.error);load()};
window.rejectPay=async function(id){if(!confirm('Reject this payment request?'))return;var note=prompt('Reason (optional):')||'';await fetch('/api/admin/payment-requests/'+id+'/reject',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({note:note})});load()};
load();<\/script></body></html>`;

// ROUTES
app.get('/',(req,res)=>res.type('html').send(APP_HTML));
app.get('/login',(req,res)=>res.type('html').send(LOGIN_HTML));
app.get('/login.html',(req,res)=>res.type('html').send(LOGIN_HTML));
app.get('/admin',(req,res)=>res.type('html').send(ADMIN_HTML));
app.get('/admin.html',(req,res)=>res.type('html').send(ADMIN_HTML));

app.post('/api/auth/register',async(req,res)=>{
  const{email,username,password}=req.body||{};
  if(!email||!username||!password)return res.status(400).json({error:'Missing fields'});
  if(password.length<8)return res.status(400).json({error:'Password 8+ chars'});
  if(!/^[a-zA-Z0-9_]{3,20}$/.test(username))return res.status(400).json({error:'Username 3-20 chars'});
  const c=await getOne('SELECT COUNT(*) AS c FROM users');
  const isFirst=Number(c.c)===0?1:0;
  try{
    const uid=await insertId('INSERT INTO users(email,username,password_hash,is_admin) VALUES(?,?,?,?)',[email.toLowerCase(),username,hashPassword(password),isFirst]);
    await run('INSERT INTO accounts(user_id,balance) VALUES(?,?)',[uid,FREE_BALANCE]);
    await grantSubscription(uid,FREE_BALANCE,0,99999);
    const u=await getOne('SELECT * FROM users WHERE id=?',[uid]);
    res.cookie('token',signToken(u),{httpOnly:true,sameSite:'lax',secure:IS_PROD,maxAge:30*24*3600*1000});
    res.json({ok:true,user:{id:u.id,username:u.username,isAdmin:!!u.is_admin}});
  }catch(e){
    if(String(e).includes('UNIQUE')||String(e).includes('duplicate'))return res.status(409).json({error:'Username or email taken'});
    console.error(e);res.status(500).json({error:'Server error'});
  }
});
app.post('/api/auth/login',async(req,res)=>{
  const{username,password}=req.body||{};
  const u=await getOne('SELECT * FROM users WHERE username=? OR email=?',[username,(username||'').toLowerCase()]);
  if(!u||!verifyPassword(password,u.password_hash))return res.status(401).json({error:'Invalid credentials'});
  if(u.totp_enabled&&u.totp_secret)return res.json({ok:true,requires2FA:true,tempToken:signTemp(u.id)});
  res.cookie('token',signToken(u),{httpOnly:true,sameSite:'lax',secure:IS_PROD,maxAge:30*24*3600*1000});
  res.json({ok:true,user:{id:u.id,username:u.username,isAdmin:!!u.is_admin}});
});
app.post('/api/auth/verify-2fa',async(req,res)=>{
  const{tempToken,code}=req.body||{};
  if(!tempToken||!code)return res.status(400).json({error:'Missing'});
  let p;try{p=jwt.verify(tempToken,JWT_SECRET)}catch{return res.status(401).json({error:'Expired'})}
  if(p.p!=='2fa')return res.status(401).json({error:'Bad token'});
  const u=await getOne('SELECT * FROM users WHERE id=?',[p.uid]);
  if(!u?.totp_secret)return res.status(401).json({error:'User'});
  if(!authenticator.verify({token:String(code),secret:u.totp_secret}))return res.status(401).json({error:'Bad code'});
  res.cookie('token',signToken(u),{httpOnly:true,sameSite:'lax',secure:IS_PROD,maxAge:30*24*3600*1000});
  res.json({ok:true,user:{id:u.id,username:u.username,isAdmin:!!u.is_admin}});
});
app.post('/api/auth/logout',(req,res)=>{res.clearCookie('token');res.json({ok:true})});
app.get('/api/auth/me',authRequired,(req,res)=>{res.json({user:{id:req.user.id,email:req.user.email,username:req.user.username,isAdmin:!!req.user.is_admin,totpEnabled:!!req.user.totp_enabled}})});
app.post('/api/auth/2fa/setup',authRequired,adminRequired,async(req,res)=>{
  const secret=authenticator.generateSecret();
  await run('UPDATE users SET totp_secret=?,totp_enabled=0 WHERE id=?',[secret,req.user.id]);
  res.json({ok:true,secret,otpauth:authenticator.keyuri(req.user.username,'TradeHub',secret)});
});
app.post('/api/auth/2fa/enable',authRequired,adminRequired,async(req,res)=>{
  const{code}=req.body||{};
  const u=await getOne('SELECT totp_secret FROM users WHERE id=?',[req.user.id]);
  if(!u?.totp_secret)return res.status(400).json({error:'Run setup first'});
  if(!authenticator.verify({token:String(code),secret:u.totp_secret}))return res.status(400).json({error:'Bad code'});
  await run('UPDATE users SET totp_enabled=1 WHERE id=?',[req.user.id]);
  res.json({ok:true});
});
app.post('/api/auth/2fa/disable',authRequired,adminRequired,async(req,res)=>{
  await run('UPDATE users SET totp_enabled=0,totp_secret=NULL WHERE id=?',[req.user.id]);
  res.json({ok:true});
});

// SUBSCRIPTION ROUTES
app.get('/api/subscription',authRequired,async(req,res)=>{
  await ensureDefaultSub(req.user.id);
  const sub=await getActiveSub(req.user.id);
  const pending=await getOne(`SELECT * FROM payment_requests WHERE user_id=? AND status='pending' ORDER BY id DESC`,[req.user.id]);
  res.json({
    subscription:sub,
    balanceLimit:sub?sub.balance_limit:FREE_BALANCE,
    isPro:sub?sub.balance_limit>FREE_BALANCE:false,
    pendingRequest:pending||null,
    paybill:PAYBILL_NUMBER,
    paybillAccount:PAYBILL_ACCOUNT,
    rate:RATE_PER_1K_KES,
    minBalance:MIN_BALANCE,
    maxBalance:MAX_BALANCE,
    freeBalance:FREE_BALANCE
  });
});
app.get('/api/subscription/quote',authRequired,(req,res)=>{
  const balance=Number(req.query.balance)||FREE_BALANCE;
  res.json(quoteFor(balance));
});
app.post('/api/subscription/request',authRequired,async(req,res)=>{
  const{balance_limit,reference,method}=req.body||{};
  if(!balance_limit||!reference)return res.status(400).json({error:'Balance and reference required'});
  const q=quoteFor(balance_limit);
  if(q.isFree)return res.status(400).json({error:'Free balance needs no payment'});
  const existing=await getOne(`SELECT * FROM payment_requests WHERE user_id=? AND status='pending'`,[req.user.id]);
  if(existing)return res.status(409).json({error:'You already have a pending payment. Wait for approval.'});
  const id=await insertId(`INSERT INTO payment_requests(user_id,balance_limit,amount_kes,method,reference,status) VALUES(?,?,?,?,?,?)`,[req.user.id,q.balance,q.price_kes,method||'mpesa',String(reference).slice(0,100),'pending']);
  res.json({ok:true,id,balance:q.balance,amount:q.price_kes});
});
app.get('/api/admin/payment-requests',authRequired,adminRequired,async(req,res)=>{
  const rows=await query(`SELECT p.*,u.username,u.email FROM payment_requests p JOIN users u ON u.id=p.user_id ORDER BY CASE WHEN p.status='pending' THEN 0 ELSE 1 END, p.id DESC LIMIT 200`);
  res.json({requests:rows});
});
app.post('/api/admin/payment-requests/:id/approve',authRequired,adminRequired,async(req,res)=>{
  const r=await getOne(`SELECT * FROM payment_requests WHERE id=? AND status='pending'`,[req.params.id]);
  if(!r)return res.status(404).json({error:'Not found'});
  const expires=await grantSubscription(r.user_id,r.balance_limit,r.amount_kes,30);
  await run('UPDATE accounts SET balance=? WHERE user_id=?',[r.balance_limit,r.user_id]);
  await run(`UPDATE payment_requests SET status='approved',reviewed_at=?,admin_note=? WHERE id=?`,[Math.floor(Date.now()/1000),String(req.body?.note||'').slice(0,300),r.id]);
  res.json({ok:true,expiresAt:expires});
});
app.post('/api/admin/payment-requests/:id/reject',authRequired,adminRequired,async(req,res)=>{
  const r=await getOne(`SELECT * FROM payment_requests WHERE id=? AND status='pending'`,[req.params.id]);
  if(!r)return res.status(404).json({error:'Not found'});
  await run(`UPDATE payment_requests SET status='rejected',reviewed_at=?,admin_note=? WHERE id=?`,[Math.floor(Date.now()/1000),String(req.body?.note||'').slice(0,300),r.id]);
  res.json({ok:true});
});

// MARKET
app.get('/api/instruments',(req,res)=>{
  const out=INSTRUMENTS.map(x=>{const mid=livePrices[x.symbol]||x.start;const{bid,ask}=getBidAsk(x.symbol,mid);return{symbol:x.symbol,base:x.base,quote:x.quote,cat:x.cat,bid,ask,mid,leverage:CATS[x.cat].lev}});
  res.json({instruments:out});
});
app.get('/api/signals/:symbol',async(req,res)=>{
  const sym=(req.params.symbol||'').toUpperCase();
  const interval=String(req.query.interval||'15m');
  const candles=await fetchKlines(sym,interval,100);
  if(!candles||candles.length<30)return res.json({signal:null,candles:candles?candles.length:0});
  const closes=candles.map(c=>c.c);
  const sig=computeSignal(closes);
  res.json({signal:sig,candles:candles.length,interval});
});app.get('/api/klines/:symbol',async(req,res)=>{
  const sym=(req.params.symbol||'').toUpperCase();
  const interval=String(req.query.interval||'4h');
  const limit=Math.min(Number(req.query.limit)||100,500);
  res.json({candles:await fetchKlines(sym,interval,limit)});
});
app.get('/api/account',authRequired,async(req,res)=>res.json(await accountStats(req.user.id)));
app.get('/api/positions',authRequired,async(req,res)=>{
  const rows=await query("SELECT * FROM positions WHERE user_id=? AND status='open' ORDER BY id DESC",[req.user.id]);
  res.json({positions:rows.map(p=>{const mid=livePrices[p.symbol];const{bid,ask}=getBidAsk(p.symbol,mid);const closePrice=p.side==='long'?bid:ask;const pnl=(closePrice-p.entry_price)*p.size*(p.side==='long'?1:-1);const pnlPct=p.entry_price*p.size>0?(pnl/(p.entry_price*p.size))*100:0;return{id:p.id,symbol:p.symbol,side:p.side,size:p.size,entry:p.entry_price,sl:p.sl,tp:p.tp,bid,ask,closePrice,pnl,pnlPct,openedAt:p.opened_at,leverage:CATS[INST_MAP[p.symbol].cat].lev}})});
});
app.post('/api/positions',authRequired,async(req,res)=>{
  const{symbol,side,size,sl,tp}=req.body||{};
  try{const r=await openMarket(req.user.id,(symbol||'').toUpperCase(),side,Number(size),sl?Number(sl):null,tp?Number(tp):null);res.json({ok:true,...r})}
  catch(e){res.status(400).json({error:e.message})}
});
app.post('/api/positions/:id/close',authRequired,async(req,res)=>{
  const pos=await getOne("SELECT * FROM positions WHERE id=? AND user_id=? AND status='open'",[req.params.id,req.user.id]);
  if(!pos)return res.status(404).json({error:'Not found'});
  const mid=livePrices[pos.symbol];const{bid,ask}=getBidAsk(pos.symbol,mid);
  const closePrice=pos.side==='long'?bid:ask;
  const pnl=await closePosition(pos,closePrice,'manual');
  res.json({ok:true,pnl,closePrice});
});
app.get('/api/pending',authRequired,async(req,res)=>{
  const rows=await query("SELECT * FROM pending_orders WHERE user_id=? AND status='pending' ORDER BY id DESC",[req.user.id]);
  res.json({pending:rows});
});
app.post('/api/pending',authRequired,async(req,res)=>{
  if(!await isPro(req.user.id))return res.status(402).json({error:'Pro subscription required for pending orders'});
  const{symbol,type,price,size,sl,tp}=req.body||{};
  const sym=(symbol||'').toUpperCase();
  if(!INST_MAP[sym])return res.status(400).json({error:'Unknown symbol'});
  if(!['buy_limit','sell_limit','buy_stop','sell_stop'].includes(type))return res.status(400).json({error:'Invalid type'});
  const p=Number(price),s=Number(size);
  if(!(p>0)||!(s>0))return res.status(400).json({error:'Invalid price or size'});
  const id=await insertId(`INSERT INTO pending_orders(user_id,symbol,type,price,size,sl,tp,status,created_at) VALUES(?,?,?,?,?,?,?,?,?)`,[req.user.id,sym,type,p,s,sl?Number(sl):null,tp?Number(tp):null,'pending',Math.floor(Date.now()/1000)]);
  res.json({ok:true,id});
});
app.delete('/api/pending/:id',authRequired,async(req,res)=>{
  const o=await getOne("SELECT * FROM pending_orders WHERE id=? AND user_id=? AND status='pending'",[req.params.id,req.user.id]);
  if(!o)return res.status(404).json({error:'Not found'});
  await run("UPDATE pending_orders SET status='cancelled' WHERE id=?",[o.id]);
  res.json({ok:true});
});
app.get('/api/history',authRequired,async(req,res)=>{
  const rows=await query("SELECT * FROM positions WHERE user_id=? AND status='closed' ORDER BY closed_at DESC LIMIT 100",[req.user.id]);
  res.json({history:rows});
});
app.get('/api/admin/users',authRequired,adminRequired,async(req,res)=>{
  const users=await query('SELECT id,email,username,is_admin,totp_enabled,created_at FROM users ORDER BY id ASC');
  const accounts=await query('SELECT user_id,balance FROM accounts');
  const acctMap=Object.fromEntries(accounts.map(a=>[a.user_id,a.balance]));
  res.json({users:users.map(u=>({...u,balance:acctMap[u.id]||0}))});
});
app.post('/api/admin/users/:id/promote',authRequired,adminRequired,async(req,res)=>{await run('UPDATE users SET is_admin=1 WHERE id=?',[req.params.id]);res.json({ok:true})});
app.post('/api/admin/users/:id/demote',authRequired,adminRequired,async(req,res)=>{
  if(Number(req.params.id)===req.user.id)return res.status(400).json({error:"Can't demote yourself"});
  await run('UPDATE users SET is_admin=0 WHERE id=?',[req.params.id]);res.json({ok:true});
});
app.post('/api/admin/users/:id/credit',authRequired,adminRequired,async(req,res)=>{
  const amt=Number(req.body?.amount)||0;
  if(!amt)return res.status(400).json({error:'Amount required'});
  await run('UPDATE accounts SET balance=balance+? WHERE user_id=?',[amt,req.params.id]);
  res.json({ok:true});
});
app.get('/api/admin/positions',authRequired,adminRequired,async(req,res)=>{
  const rows=await query(`SELECT p.*,u.username FROM positions p JOIN users u ON u.id=p.user_id ORDER BY p.id DESC LIMIT 200`);
  res.json({positions:rows});
});

// WS
const wss=new WebSocket.Server({server,path:'/ws'});
const clients=new Set();
wss.on('connection',ws=>{clients.add(ws);INSTRUMENTS.forEach(i=>{const mid=livePrices[i.symbol]||i.start;try{ws.send(JSON.stringify({symbol:i.symbol,mid}))}catch{}});ws.on('close',()=>clients.delete(ws));ws.on('error',()=>clients.delete(ws))});
function broadcast(obj){const msg=JSON.stringify(obj);for(const c of clients)if(c.readyState===1)try{c.send(msg)}catch{}}

function startBinance(){
  const streams=INSTRUMENTS.filter(i=>i.cat==='crypto').map(i=>i.symbol.toLowerCase()+'@trade').join('/');
  const url='wss://stream.binance.com:9443/stream?streams='+streams;
  let up;try{up=new WebSocket(url)}catch{return}
  up.on('message',raw=>{try{const e=JSON.parse(raw);const d=e.data||e;if(d.s&&d.p){livePrices[d.s]=parseFloat(d.p);broadcast({symbol:d.s,mid:livePrices[d.s]})}}catch{}});
  up.on('close',()=>setTimeout(startBinance,5000));
  up.on('error',()=>{});
}
function startSimulator(){
  setInterval(()=>{for(const i of INSTRUMENTS){const cur=livePrices[i.symbol]||i.start;const vol=cur*0.0008;livePrices[i.symbol]=cur+(Math.random()-0.5)*vol;broadcast({symbol:i.symbol,mid:livePrices[i.symbol]})}},1500);
}

(async()=>{
  try{
    await initDB();
    server.listen(PORT,()=>{
      console.log(`✅ TradeHub running on port ${PORT}`);
      console.log(`   Free tier: $${FREE_BALANCE} · Rate: KES ${RATE_PER_1K_KES} per $1,000/mo`);
      startBinance();
      startSimulator();
      setInterval(processTicks,1000);
    });
  }catch(e){console.error('Startup failed:',e);process.exit(1)}
})();
