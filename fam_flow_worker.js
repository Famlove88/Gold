/* ============================================================
   FAM Flow — Cloudflare Worker (v5 / phase 1)
   24/7 companion for index.html (FAM Flow):
     - Cron (every 5 min): price alerts, extreme funding, OI jumps, ⚡ news → Telegram
     - Aggregates the liquidations / whale counters the page pushes (rolling 24h, per symbol)
     - Relays Telegram messages for the page (so the bot token never sits in the browser)
   v10.3: data gateway — GET /news?coin=XRP · /fng · /gold · /cg (all cached via the Cache API; the page tries the worker first, direct sources remain as fallback)
   v10: on-chain exchange flows (Etherscan + TronGrid), conditional-scenario watcher 24/7, wallet labels.
   Crons: every 5 min = alerts + scenario checks; every 10 min = on-chain ledger (branch on event.cron)
   HTTP (all CORS *):
     GET  /chain                      → on-chain ledger {ev[], agg24, lastRun, wallets:{n}}
     POST /wallets  {key,eth:{addr:label},tron:{addr:label}}        (replaces the exchange wallet map)
     POST /cond     {key,conds:[{id,pair,tf,kind,level,created}]}   (replaces watched scenarios)
     GET  /health                     → {ok, symbols, lastCron}
     GET  /state?symbol=BTCUSDT       → {agg24:{liqL,liqS,whB,whS}, alerts, lastPush}
     POST /push     {key,symbol,liq:{long,short},whale:{buy,sell}}   (deltas since last push)
     POST /alerts   {key,alerts:[{pair,dir,price,fired?}]}            (replaces the alert list)
     POST /telegram {key,text}                                        (HTML text)
     GET  /run?key=FAM_KEY            → run the cron job now
   Bindings (wrangler_flow.toml): KV FLOW_KV; vars SYMBOLS; secrets TG_TOKEN, TG_CHAT, FAM_KEY
   Free plan budget: KV ≤1000 writes/day → the page pushes every 5 min (288/day) and the cron
   writes only when something changed. Subrequests ≤50/run → keep SYMBOLS ≤ 8.
   ============================================================ */
const SPOT=['https://data-api.binance.vision','https://api.binance.com'];
const FUT='https://fapi.binance.com';
const CORS={'access-control-allow-origin':'*','access-control-allow-methods':'GET,POST,OPTIONS','access-control-allow-headers':'content-type'};
const json=(o,st=200)=>new Response(JSON.stringify(o),{status:st,headers:{'content-type':'application/json',...CORS}});
const HOT=[['fomc',3],['federal reserve',3],['fed ',3],['fed\'s',3],['powell',3],['warsh',3],['rate cut',3],['rate hike',3],['interest rate',3],['cpi',3],['inflation',2],['tariff',3],['sec ',3],['sec\'s',3],['etf',3],['hack',3],['exploit',3],['ban ',2],['bans ',2],['lawsuit',2],['bankrupt',3],['liquidat',2],['crash',3],['plunge',3],['surge',2],['record high',3],['all-time high',3],['halt',3],['depeg',3],['tether',2],['binance',2],['coinbase',2],['blackrock',2],['trump',2],['war ',2],['sanction',2],['regulat',2],['treasury',2],['jobs report',3],['nonfarm',3],['payroll',3],['recession',3],['default',2],['iran',2],['halving',2],['outflow',2],['inflow',2],['ceasefire',2],['nuclear',2]];
const BULL=['surge','soar','rally','rallies','record','all-time high','approve','approval','inflow','buys ','buying','accumulat','adopt','rate cut','cuts rates','cut rates','bullish','jump','gain','rebound','recover','breaks above','tops ','hits $','dovish','ease','easing','stimulus','climb','rise','rises','higher'];
const BEAR=['plunge','crash','drop','drops','falls','fall ','dump','hack','exploit','ban ','bans ','lawsuit','sue ','sues','outflow','liquidat','bankrupt','sell-off','selloff','bearish','slump','tumble','sink','rate hike','hikes','hawkish','war ','sanction','reject','delay','warning','fear','panic','breaks below','slide','loses','lower','decline','tariff'];
const fp=n=>Number(n).toLocaleString('en-US',{maximumFractionDigits:n<1?5:n<100?3:2});
const usd=n=>Math.abs(n)>=1e9?'$'+(n/1e9).toFixed(2)+'B':Math.abs(n)>=1e6?'$'+(n/1e6).toFixed(2)+'M':Math.abs(n)>=1e3?'$'+(n/1e3).toFixed(1)+'K':'$'+Number(n).toFixed(0);

async function spotGet(path){for(const b of SPOT){try{const r=await fetch(b+path,{cf:{cacheTtl:0}});if(r.ok)return await r.json();}catch(e){}}throw new Error('spot unavailable');}
async function futGet(path){const r=await fetch(FUT+path);if(!r.ok)throw new Error('fut '+r.status);return r.json();}
async function kvGet(env,k,def){try{const v=await env.FLOW_KV.get(k,'json');return v==null?def:v;}catch(e){return def;}}
async function kvPut(env,k,v){await env.FLOW_KV.put(k,JSON.stringify(v));}
async function tg(env,text){if(!env.TG_TOKEN||!env.TG_CHAT)return false;try{const r=await fetch(`https://api.telegram.org/bot${env.TG_TOKEN}/sendMessage`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({chat_id:env.TG_CHAT,text,parse_mode:'HTML',disable_web_page_preview:true})});return r.ok;}catch(e){return false;}}
const symbols=env=>(env.SYMBOLS||'BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,PAXGUSDT').split(',').map(s=>s.trim().toUpperCase()).filter(Boolean).slice(0,8);
const okKey=(env,k)=>!env.FAM_KEY||k===env.FAM_KEY;

/* ---------- rolling 24h aggregation of page-pushed counters (15-min buckets) ---------- */
function bucketOf(t){return Math.floor(t/900e3)*900e3;}
function agg24(buckets){const cut=Date.now()-86400e3;const a={liqL:0,liqS:0,whB:0,whS:0};for(const b of buckets){if(b.t<cut)continue;a.liqL+=b.liqL||0;a.liqS+=b.liqS||0;a.whB+=b.whB||0;a.whS+=b.whS||0;}return a;}
async function handlePush(env,body){
  const sym=String(body.symbol||'').toUpperCase();if(!sym)return json({error:'symbol'},400);
  const all=await kvGet(env,'agg',{});const arr=(all[sym]||[]).filter(b=>b.t>Date.now()-86400e3-900e3);
  const bt=bucketOf(Date.now());let b=arr.find(x=>x.t===bt);if(!b){b={t:bt,liqL:0,liqS:0,whB:0,whS:0};arr.push(b);}
  const L=body.liq||{},Wh=body.whale||{};b.liqL+=Math.max(0,+L.long||0);b.liqS+=Math.max(0,+L.short||0);b.whB+=Math.max(0,+Wh.buy||0);b.whS+=Math.max(0,+Wh.sell||0);
  all[sym]=arr;all._lastPush=Date.now();await kvPut(env,'agg',all);
  return json({ok:true,agg24:agg24(arr),lastPush:all._lastPush});
}

/* ---------- cron: alerts ---------- */
async function cron(env){
  const syms=symbols(env),st=await kvGet(env,'cron',{fund:{},oi:{},news:[],lastRun:0});const msgs=[];let changed=false;
  // 1) price alerts (synced from the page)
  let alerts=await kvGet(env,'alerts',[]);const pairs=[...new Set(alerts.filter(a=>!a.fired).map(a=>a.pair))];
  if(pairs.length){try{const px=await spotGet(`/api/v3/ticker/price?symbols=${encodeURIComponent(JSON.stringify(pairs))}`);const map=Object.fromEntries(px.map(x=>[x.symbol,+x.price]));let ch=false;
    for(const a of alerts){if(a.fired)continue;const p=map[a.pair];if(!p)continue;if((a.dir==='above'&&p>=a.price)||(a.dir==='below'&&p<=a.price)){a.fired=Date.now();ch=true;msgs.push(`🔔 <b>${a.pair.replace('USDT','')} ${a.dir==='above'?'طلع فوق':'نزل تحت'} ${fp(a.price)}</b>\nالسعر الآن ${fp(p)}`);}}
    if(ch)await kvPut(env,'alerts',alerts);}catch(e){}}
  // 2) funding extremes + 3) OI jumps (one premiumIndex call for all, one OI call per symbol)
  try{const pi=await futGet('/fapi/v1/premiumIndex');const byS=Object.fromEntries(pi.map(x=>[x.symbol,x]));
    for(const s of syms){const x=byS[s];if(!x)continue;const f=+x.lastFundingRate*100;
      if(Math.abs(f)>0.05&&Date.now()-(st.fund[s]||0)>8*3600e3){st.fund[s]=Date.now();changed=true;msgs.push(`⚠️ <b>Funding متطرّف على ${s.replace('USDT','')}</b>: ${f>0?'+':''}${f.toFixed(4)}% — ${f>0?'longs مزدحمين، خطر تصفية للتحت':'shorts مزدحمين، فرصة squeeze للفوق'}`);}}
  }catch(e){}
  for(const s of syms){try{const h=await futGet(`/futures/data/openInterestHist?symbol=${s}&period=5m&limit=13`);if(h.length>=13){const a=+h[0].sumOpenInterestValue,b=+h[h.length-1].sumOpenInterestValue,ch=(b-a)/a*100;
      if(Math.abs(ch)>=3&&Date.now()-(st.oi[s]||0)>2*3600e3){st.oi[s]=Date.now();changed=true;msgs.push(`📈 <b>OI ${s.replace('USDT','')} ${ch>0?'زاد':'نقص'} ${Math.abs(ch).toFixed(1)}% بساعة</b> (${usd(a)} → ${usd(b)}) — ${ch>0?'مال جديد داخل عالرافعة':'خروج/تصفية من الرافعة'}`);}}}catch(e){}}
  // 4) ⚡ news (Google News RSS, no CORS issue inside a worker)
  try{const feeds=['https://news.google.com/rss/search?q=bitcoin%20OR%20crypto%20OR%20ethereum&hl=en-US&gl=US&ceid=US:en','https://news.google.com/rss/search?q=Fed%20OR%20%22interest%20rates%22%20OR%20inflation%20OR%20CPI%20OR%20tariffs&hl=en-US&gl=US&ceid=US:en'];
    const seen=new Set(st.news||[]);let sent=0;const hourAgo=Date.now()-3600e3;
    for(const f of feeds){const r=await fetch(f,{headers:{'user-agent':'Mozilla/5.0 FAMFlow'}});if(!r.ok)continue;const xml=await r.text();
      const items=[...xml.matchAll(/<item>([\s\S]*?)<\/item>/g)].map(m=>{const g=t=>{const x=m[1].match(new RegExp('<'+t+'>(?:<!\\[CDATA\\[)?([\\s\\S]*?)(?:\\]\\]>)?</'+t+'>'));return x?x[1].trim():'';};return{title:g('title'),link:g('link'),t:new Date(g('pubDate')).getTime()||0};});
      for(const it of items){if(it.t<hourAgo)continue;const tl=' '+it.title.toLowerCase()+' ';let imp=0;for(const [k,w] of HOT)if(tl.includes(k))imp=Math.max(imp,w);let tone=0;for(const k of BULL)if(tl.includes(k))tone++;for(const k of BEAR)if(tl.includes(k))tone--;
        const key=it.title.toLowerCase().replace(/[^a-z0-9]/g,'').slice(0,60);if(imp>=3&&tone!==0&&!seen.has(key)&&sent<6){seen.add(key);sent++;changed=true;msgs.push(`⚡ <b>خبر مؤثر</b> ${tone>0?'▲':'▼'}\n${it.title}\n${it.link}`);}}}
    st.news=[...seen].slice(-300);}catch(e){}
  // 4b) scenario watcher
  try{const cm=await condCron(env,st);msgs.push(...cm);}catch(e){}
  // 5) morning report (once a day at REPORT_UTC hour, default 05:00 UTC = 08:00 Beirut in winter / 07:00 in summer)
  const rh=+(env.REPORT_UTC||5),dnow=new Date(),dkey=dnow.toISOString().slice(0,10);
  if(dnow.getUTCHours()===rh&&st.reportDay!==dkey){st.reportDay=dkey;changed=true;try{const rep=await liteReport(env,syms);if(rep)msgs.push(rep);}catch(e){}}
  st.lastRun=Date.now();if(changed||!st.saved){st.saved=true;await kvPut(env,'cron',st);}
  for(const m of msgs)await tg(env,m);
  return{sent:msgs.length};
}
/* ---------- lite morning report (no engine in the worker: trend + delta + funding + OI + F&G + events + headlines) ---------- */
const ETd=(y,m,d,hh,mm)=>{const dst=(()=>{const n=k=>{const dow=new Date(Date.UTC(y,k,1)).getUTCDay();return 1+((7-dow)%7);};const t=Date.UTC(y,m-1,d);return t>=Date.UTC(y,2,n(2)+7)&&t<Date.UTC(y,10,n(10));})();return Date.UTC(y,m-1,d,hh,mm)+(dst?4:5)*3600e3;};
const CAL_HI=[['FOMC',2026,10,28,14,0],['FOMC',2026,12,9,14,0],['FOMC',2027,1,27,14,0],['FOMC',2027,3,17,14,0],['FOMC',2027,4,28,14,0],['FOMC',2027,6,9,14,0],['FOMC',2027,7,28,14,0],['FOMC',2027,9,15,14,0],['FOMC',2027,10,27,14,0],['FOMC',2027,12,8,14,0],['CPI',2026,11,10,8,30],['CPI',2026,12,10,8,30],['NFP',2026,11,6,8,30],['NFP',2026,12,4,8,30]].map(([t,y,m,d,hh,mm])=>({t,at:ETd(y,m,d,hh,mm)}));
function liteBias(kl){const cs=kl.map(k=>({c:+k[4],v:+k[5],d:2*(+k[9])-(+k[5])}));const closes=cs.map(x=>x.c);let e=closes[0];const kk=2/21;for(let i=1;i<closes.length;i++)e=closes[i]*kk+e*(1-kk);const p=closes[closes.length-1];const d10=cs.slice(-10).reduce((a,x)=>a+x.d,0),v10=cs.slice(-10).reduce((a,x)=>a+x.v,0)||1;const s=Math.max(-1,Math.min(1,d10/v10*4))*0.6+Math.max(-1,Math.min(1,(p-e)/e*40))*0.4;return{s,label:s>0.2?'صعودي':s<-0.2?'هبوطي':'محايد',prob:Math.round(50+s*35)};}
async function liteReport(env,syms){
  const now=Date.now();const pi=await futGet('/fapi/v1/premiumIndex').catch(()=>[]);const byS=Object.fromEntries((pi||[]).map(x=>[x.symbol,x]));
  const lines=[`<b>☀️ FAM Flow — تقرير الصباح ${new Date().toLocaleDateString('en-GB')}</b> <i>(قراءة مبسّطة من الـ worker — التفصيل بالصفحة)</i>`];
  for(const s of syms){try{const [tk,kl,oh]=await Promise.all([spotGet(`/api/v3/ticker/24hr?symbol=${s}`),spotGet(`/api/v3/klines?symbol=${s}&interval=1h&limit=60`),futGet(`/futures/data/openInterestHist?symbol=${s}&period=1h&limit=25`).catch(()=>null)]);
      const b=liteBias(kl),f=byS[s]?(+byS[s].lastFundingRate*100).toFixed(4)+'%':'—';const oi=oh&&oh.length>=2?(+oh[oh.length-1].sumOpenInterestValue-+oh[0].sumOpenInterestValue):null;
      lines.push(`<b>${s.replace('USDT','')}</b> ${fp(+tk.lastPrice)} (${(+tk.priceChangePercent)>0?'+':''}${(+tk.priceChangePercent).toFixed(2)}%) · ${b.label} ${b.s<0?100-b.prob:b.prob}% · funding ${f}${oi!=null?' · OI$ 24h '+(oi>0?'+':'')+usd(oi):''}`);}catch(e){}}
  try{const r=await fetch('https://api.alternative.me/fng/?limit=1');if(r.ok){const j=await r.json();const x=j.data&&j.data[0];if(x)lines.push(`Fear & Greed ${x.value} (${x.value_classification})`);}}catch(e){}
  const ev=CAL_HI.filter(e=>e.at>now-3600e3&&e.at<now+30*3600e3);lines.push(ev.length?'⚠️ أحداث خلال 30 ساعة: '+ev.map(e=>e.t+' بعد '+((e.at-now)/3600e3).toFixed(1)+' س').join(' · '):'أحداث عالية التأثير خلال 30 ساعة: لا شي');
  try{const r=await fetch('https://news.google.com/rss/search?q=bitcoin%20OR%20crypto&hl=en-US&gl=US&ceid=US:en',{headers:{'user-agent':'Mozilla/5.0 FAMFlow'}});if(r.ok){const xml=await r.text();const tt=[...xml.matchAll(/<item>[\s\S]*?<title>(?:<!\[CDATA\[)?([\s\S]*?)(?:\]\]>)?<\/title>/g)].map(m=>m[1].trim()).slice(0,3);if(tt.length)lines.push('📰 '+tt.join('\n📰 '));}}catch(e){}
  return lines.join('\n');
}

/* ---------- phase 2: cross-exchange / options / macro aggregation (Cache API, no KV writes) ---------- */
async function cached(key,ttl,fn){
  let cache=null;try{cache=caches.default;}catch(e){}
  const creq=new Request('https://cache.famflow.local/'+key);
  if(cache){try{const hit=await cache.match(creq);if(hit)return hit.json();}catch(e){}}
  const data=await fn();
  if(cache){try{await cache.put(creq,new Response(JSON.stringify(data),{headers:{'content-type':'application/json','cache-control':'max-age='+ttl}}));}catch(e){}}
  return data;
}
const getJ=async(u,opt)=>{const r=await fetch(u,Object.assign({headers:{'user-agent':'Mozilla/5.0 FAMFlow','accept':'application/json'}},opt||{}));if(!r.ok)throw new Error(u.split('/')[2]+' '+r.status);return r.json();};
const safe=p=>p.then(v=>v).catch(()=>null);
async function multi(sym){
  const base=sym.replace('USDT',''),now=Date.now();
  const [bPx,bPi,bOi,by,okT,okOi,okF,okL,okI,hl,cb]=await Promise.all([
    safe(getJ(SPOT[0]+`/api/v3/ticker/price?symbol=${sym}`)),safe(getJ(FUT+`/fapi/v1/premiumIndex?symbol=${sym}`)),safe(getJ(FUT+`/fapi/v1/openInterest?symbol=${sym}`)),
    safe(getJ(`https://api.bybit.com/v5/market/tickers?category=linear&symbol=${sym}`)),
    safe(getJ(`https://www.okx.com/api/v5/market/ticker?instId=${base}-USDT-SWAP`)),safe(getJ(`https://www.okx.com/api/v5/public/open-interest?instType=SWAP&instId=${base}-USDT-SWAP`)),safe(getJ(`https://www.okx.com/api/v5/public/funding-rate?instId=${base}-USDT-SWAP`)),
    safe(getJ(`https://www.okx.com/api/v5/public/liquidation-orders?instType=SWAP&uly=${base}-USDT&state=filled&limit=100`)),safe(getJ(`https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=${base}-USDT-SWAP`)),
    safe(getJ('https://api.hyperliquid.xyz/info',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({type:'metaAndAssetCtxs'})})),
    safe(getJ(`https://api.exchange.coinbase.com/products/${base}-USD/ticker`))]);
  const out={t:now,symbol:sym,ex:{}};
  if(bPx&&bPi){const px=+bPx.price,oiB=bOi?+bOi.openInterest:null;out.ex.binance={price:px,mark:+bPi.markPrice,oiUsd:oiB!=null?oiB*(+bPi.markPrice):null,fund8h:+bPi.lastFundingRate*100};}
  if(by&&by.result&&by.result.list&&by.result.list[0]){const x=by.result.list[0];out.ex.bybit={price:+x.lastPrice,oiUsd:+x.openInterestValue||(+x.openInterest*+x.lastPrice),fund8h:+x.fundingRate*100,vol24:+x.turnover24h};}
  if(okT&&okT.data&&okT.data[0]){const x=okT.data[0];const oi=okOi&&okOi.data&&okOi.data[0];const f=okF&&okF.data&&okF.data[0];out.ex.okx={price:+x.last,oiUsd:oi?(+oi.oiUsd||(+oi.oiCcy*+x.last)):null,fund8h:f?+f.fundingRate*100:null,vol24:+x.volCcy24h*+x.last};
    const ctVal=okI&&okI.data&&okI.data[0]?+okI.data[0].ctVal:null;if(okL&&okL.data&&okL.data[0]&&ctVal){let L=0,S=0;for(const d of okL.data[0].details||[]){if(now-(+d.ts)>3600e3)continue;const v=+d.sz*ctVal*+d.bkPx;if(d.side==='sell')L+=v;else S+=v;}out.ex.okx.liq1h={long:L,short:S};}}
  if(hl&&Array.isArray(hl)&&hl[0]&&hl[0].universe){const i=hl[0].universe.findIndex(u=>u.name===base);if(i>=0&&hl[1]&&hl[1][i]){const c=hl[1][i];out.ex.hyperliquid={price:+c.markPx,oiUsd:+c.openInterest*+c.markPx,fund8h:+c.funding*100*8,vol24:+c.dayNtlVlm,premium:+c.premium*100};}}
  if(cb&&cb.price)out.ex.coinbase={price:+cb.price};
  const exs=Object.entries(out.ex).filter(([k,v])=>v.oiUsd!=null&&v.fund8h!=null);
  const oiT=exs.reduce((a,[k,v])=>a+v.oiUsd,0);
  out.totals={oiUsd:oiT||null,fundW:oiT?exs.reduce((a,[k,v])=>a+v.fund8h*v.oiUsd,0)/oiT:null,fundSpread:exs.length>1?Math.max(...exs.map(x=>x[1].fund8h))-Math.min(...exs.map(x=>x[1].fund8h)):null,
    prem:out.ex.coinbase&&out.ex.binance?(out.ex.coinbase.price-out.ex.binance.price)/out.ex.binance.price*100:null,
    disp:(()=>{const ps=Object.values(out.ex).map(v=>v.price).filter(Boolean);return ps.length>1?(Math.max(...ps)-Math.min(...ps))/Math.min(...ps)*100:null;})(),
    liq1h:out.ex.okx&&out.ex.okx.liq1h?out.ex.okx.liq1h:null};
  return out;
}
function parseOpt(name){const m=String(name).match(/^([A-Z0-9]+?)(?:_USDC)?-(\d{1,2})([A-Z]{3})(\d{2})-(\d+(?:\.\d+)?)-([CP])$/);if(!m)return null;const mon={JAN:0,FEB:1,MAR:2,APR:3,MAY:4,JUN:5,JUL:6,AUG:7,SEP:8,OCT:9,NOV:10,DEC:11}[m[3]];const exp=Date.UTC(2000+ +m[4],mon,+m[2],8,0);return{cur:m[1],exp,k:+m[5],type:m[6]};}
async function options(cur){
  const now=Date.now();
  const [bs,dv]=await Promise.all([safe(getJ(`https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=${cur}&kind=option`)),safe(getJ(`https://www.deribit.com/api/v2/public/get_volatility_index_data?currency=${cur}&resolution=3600&start_timestamp=${now-8*86400e3}&end_timestamp=${now}`))]);
  if(!bs||!bs.result||!bs.result.length)throw new Error('deribit');
  const rows=bs.result.map(r=>Object.assign({},parseOpt(r.instrument_name),{oi:+r.open_interest||0,iv:+r.mark_iv||0,S:+r.underlying_price||0,vol:+r.volume||0})).filter(r=>r&&r.k&&r.exp>now);
  const S=(()=>{const a=rows.map(r=>r.S).filter(x=>x>0).sort((x,y)=>x-y);return a.length?a[Math.floor(a.length/2)]:0;})();
  const byExp={};rows.forEach(r=>{(byExp[r.exp]=byExp[r.exp]||[]).push(r);});
  const exps=Object.keys(byExp).map(Number).sort((a,b)=>a-b);
  const oiC=rows.filter(r=>r.type==='C').reduce((a,r)=>a+r.oi,0),oiP=rows.filter(r=>r.type==='P').reduce((a,r)=>a+r.oi,0);
  const maxPain=rs=>{const ks=[...new Set(rs.map(r=>r.k))].sort((a,b)=>a-b);let best=null;for(const K of ks){let loss=0;for(const r of rs){if(r.type==='C'&&K>r.k)loss+=(K-r.k)*r.oi;if(r.type==='P'&&K<r.k)loss+=(r.k-K)*r.oi;}if(!best||loss<best.loss)best={k:K,loss};}return best?best.k:null;};
  const near=exps.find(e=>e-now>2*86400e3)||exps[0];const nearRows=byExp[near]||[];
  const monthly=exps.find(e=>e-now>=7*86400e3&&new Date(e).getUTCDay()===5&&(()=>{const d=new Date(e);return d.getUTCDate()>24;})())||exps.find(e=>e-now>=20*86400e3)||near;const mRows=byExp[monthly]||[];
  const ivAt=(rs,type,target)=>{const c=rs.filter(r=>r.type===type&&r.iv>0).sort((a,b)=>Math.abs(a.k-target)-Math.abs(b.k-target));return c.length?c[0].iv:null;};
  const putIv=ivAt(mRows,'P',S*0.92),callIv=ivAt(mRows,'C',S*1.08),atm=ivAt(mRows,'C',S);
  const topStrikes=(()=>{const g={};nearRows.forEach(r=>{g[r.k]=g[r.k]||{k:r.k,oiC:0,oiP:0};g[r.k][r.type==='C'?'oiC':'oiP']+=r.oi;});return Object.values(g).sort((a,b)=>(b.oiC+b.oiP)-(a.oiC+a.oiP)).slice(0,6).sort((a,b)=>a.k-b.k);})();
  const d=dv&&dv.result&&dv.result.data||[];const lastV=d.length?d[d.length-1][4]:null;const at=h=>{const t=now-h*3600e3;let best=null;for(const x of d){if(x[0]<=t)best=x;}return best?best[4]:null;};
  const dvol=lastV!=null?{now:lastV,d1:at(24),d7:at(24*7)}:null;
  return{t:now,cur,S,dvol,pcr:{all:oiC?oiP/oiC:null,near:(()=>{const c=nearRows.filter(r=>r.type==='C').reduce((a,r)=>a+r.oi,0),p=nearRows.filter(r=>r.type==='P').reduce((a,r)=>a+r.oi,0);return c?p/c:null;})()},
    oi:{calls:oiC,puts:oiP},maxPain:{near:{exp:near,k:maxPain(nearRows)},monthly:{exp:monthly,k:maxPain(mRows)}},skew:{exp:monthly,putIv,callIv,atm,skew:putIv!=null&&callIv!=null?putIv-callIv:null},topStrikes,
    expMove:dvol?{d1:S*dvol.now/100*Math.sqrt(1/365),d7:S*dvol.now/100*Math.sqrt(7/365)}:null,nExp:exps.length};
}
async function macro(){
  const q=await safe((async()=>{const r=await fetch('https://stooq.com/q/l/?s=^spx,^vix,10usy.b,2usy.b,^dji,gc.f,cl.f,cb.f,bz.f,tlt.us&f=sd2t2ohlcv&h&e=csv',{headers:{'user-agent':'Mozilla/5.0 FAMFlow'}});if(!r.ok)throw new Error('stooq');return r.text();})());
  const out={t:Date.now(),quotes:{},hist:{}};
  if(q){q.trim().split('\n').slice(1).forEach(l=>{const c=l.split(',');if(c.length<7||c[6]==='N/D')return;out.quotes[c[0].toLowerCase()]={date:c[1],time:c[2],open:+c[3],high:+c[4],low:+c[5],close:+c[6]};});}
  for(const s of ['^spx','10usy.b','2usy.b','^vix','cl.f','cb.f','tlt.us']){const h=await safe((async()=>{const r=await fetch(`https://stooq.com/q/d/l/?s=${encodeURIComponent(s)}&i=d`,{headers:{'user-agent':'Mozilla/5.0 FAMFlow'}});if(!r.ok)throw new Error('stooq');return r.text();})());
    if(h){const rows=h.trim().split('\n').slice(1).map(l=>l.split(',')).filter(c=>c.length>=5&&c[4]!=='N/D').slice(-60).map(c=>[c[0],+c[4]]);if(rows.length)out.hist[s]=rows;}}
  // v10.3: FX series for the page's DXY proxy (ECB via Frankfurter — new domain first, legacy as fallback)
  try{const d2=new Date(),d1=new Date(Date.now()-45*86400e3),fd=d=>d.toISOString().slice(0,10);let h=await safe(getJ(`https://api.frankfurter.dev/v1/${fd(d1)}..${fd(d2)}?base=USD&symbols=EUR,JPY,GBP,CAD,SEK,CHF`));if(!h||!h.rates)h=await safe(getJ(`https://api.frankfurter.app/${fd(d1)}..${fd(d2)}?from=USD&to=EUR,JPY,GBP,CAD,SEK,CHF`));if(h&&h.rates)out.fx=h.rates;}catch(e){}
  return out;
}
/* ---------- v10.3: data gateway — the page asks the worker first (cached), direct sources stay as fallback ---------- */
const unesc=s=>String(s||'').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&quot;/g,'"').replace(/&#39;|&apos;/g,"'").replace(/&#(\d+);/g,(m,n)=>String.fromCharCode(+n));
async function rssItems(url,cat){const r=await fetch(url,{headers:{'user-agent':'Mozilla/5.0 FAMFlow','accept':'application/rss+xml,application/xml,text/xml,*/*'}});if(!r.ok)return[];const xml=await r.text();
  return [...xml.matchAll(/<item>([\s\S]*?)<\/item>/g)].map(m=>{const b=m[1];const g=t=>{const x=b.match(new RegExp('<'+t+'(?:\\s[^>]*)?>(?:<!\\[CDATA\\[)?([\\s\\S]*?)(?:\\]\\]>)?</'+t+'>'));return x?unesc(x[1].trim()):'';};
    let link=g('link');if(!link){const x=b.match(/<link[^>]*href="([^"]+)"/);if(x)link=x[1];}
    return{title:g('title'),link,t:new Date(g('pubDate')||g('dc:date')).getTime()||Date.now(),src:g('source')||g('dc:creator'),cat};}).filter(i=>i.title);}
async function newsAll(coin){const gn=q=>`https://news.google.com/rss/search?q=${encodeURIComponent(q)}&hl=en-US&gl=US&ceid=US:en`;
  const feeds=[[gn('bitcoin OR crypto OR ethereum'),'crypto'],[gn('Fed OR "interest rates" OR inflation OR CPI OR tariffs OR "stock market"'),'macro'],[gn('gold price OR XAU'),'gold'],['https://www.coindesk.com/arc/outboundfeeds/rss/','crypto'],['https://cointelegraph.com/rss','crypto']];
  if(coin&&coin!=='BTC'&&coin!=='PAXG')feeds.push([gn(coin+' crypto'),'coin']);
  const res=await Promise.all(feeds.map(([u,c])=>rssItems(u,c).catch(()=>[])));
  return{t:Date.now(),items:res.flat().sort((a,b)=>b.t-a.t).slice(0,200)};}
async function cgAll(env){const h=env.CG_KEY?{headers:{'x-cg-demo-api-key':env.CG_KEY,'accept':'application/json'}}:undefined;
  const [g,m]=await Promise.all([safe(getJ('https://api.coingecko.com/api/v3/global',h)),safe(getJ('https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=tether,usd-coin&per_page=10',h))]);
  if(!g&&!m)throw new Error('coingecko');return{t:Date.now(),global:g,stables:m};}

/* ---------- v10.5: forward test — the page's own engine (appended at download time as `ENGINE`) runs here 24/7:
   every 5 min (cron 2-57/5) one watchlist symbol gets a full engine reading; each reading is re-checked 4h later → real-world accuracy + calibration ---------- */
async function forwardStep(env){
  const st=await kvGet(env,'fwd',{snaps:[],i:0,lastRun:0});const now=Date.now();
  if(typeof ENGINE==='undefined'){st.err='ENGINE غير موجود — نزّل الـ worker من الصفحة (v10.5+) وأعد النشر';st.lastRun=now;await kvPut(env,'fwd',st);return{err:st.err};}
  const syms=symbols(env);const sym=syms[st.i%syms.length];st.i=(st.i+1)%syms.length;let evals=0;
  for(const s of st.snaps){if(evals>=6)break;if(s.after!=null||now<s.t+4*3600e3)continue;try{const k=await spotGet(`/api/v3/klines?symbol=${s.sym}&interval=1m&startTime=${s.t+4*3600e3}&limit=1`);if(k&&k.length){s.after=+k[0][4];s.ok=Math.abs(s.score)<0.12?null:((s.score>0)===(s.after>s.price));evals++;}}catch(e){}}
  try{const W=(await kvGet(env,'weights',null))||ENGINE.W_DEFAULT;
    const [k15,k1h,k4h,oiH,pi,fj]=await Promise.all([spotGet(`/api/v3/klines?symbol=${sym}&interval=15m&limit=300`),spotGet(`/api/v3/klines?symbol=${sym}&interval=1h&limit=300`),spotGet(`/api/v3/klines?symbol=${sym}&interval=4h&limit=60`),futGet(`/futures/data/openInterestHist?symbol=${sym}&period=15m&limit=8`).catch(()=>[]),futGet(`/fapi/v1/premiumIndex?symbol=${sym}`).catch(()=>null),cached('fng',600,()=>getJ('https://api.alternative.me/fng/?limit=31&format=json')).catch(()=>null)]);
    const cs=k15.map(ENGINE.parseK),c1=k1h.map(ENGINE.parseK),c4=k4h.map(ENGINE.parseK);
    const mtf={'1h':ENGINE.fMini(c1.slice(-60)),'4h':ENGINE.fMini(c4.slice(-60))};
    const nf=(a,n)=>a.slice(-n).reduce((x,c)=>x+2*c.tbq-c.qv,0),vs=(a,n)=>a.slice(-n).reduce((x,c)=>x+c.qv,0);
    const fl={s1:{net:nf(c1,1),vol:vs(c1,1)},s4:{net:nf(c1,4),vol:vs(c1,4)},f1:null,f4:null,rvol:null};
    const oiArr=(oiH||[]).map(x=>({oi:+x.sumOpenInterest,v:+x.sumOpenInterestValue,t:+x.timestamp}));
    const fng=fj&&fj.data&&fj.data[0]?+fj.data[0].value:null;
    const f=ENGINE.fFactors({candles:cs,W,mtf,ob:null,oiState:ENGINE.fOIState(oiArr,cs),funding:pi?+pi.lastFundingRate:null,taker:null,topLsr:null,lsr:null,sf:null,liq:null,flow:fl,newsTone:null,fng});
    const reg=ENGINE.fRegime(cs);const ev=CAL_HI.find(e=>e.at-now<2*3600e3&&e.at-now>-0.5*3600e3);
    const b=ENGINE.fBias(f,ENGINE.fConf({regime:reg,evHigh:!!ev,evMed:false,hot:false,shock:!!ENGINE.fShock(cs),missing:[]}),null);
    st.snaps.push({t:now,sym,price:cs[cs.length-1].close,score:+b.score.toFixed(4),prob:b.prob,label:b.label,reg:reg?reg.type:null,after:null,ok:null,v:ENGINE.v});st.err=null;
  }catch(e){st.err=String(e.message||e);}
  if(st.snaps.length>3000)st.snaps=st.snaps.slice(-3000);st.lastRun=now;await kvPut(env,'fwd',st);return{ok:true,sym,evals,n:st.snaps.length};
}
function fwdStats(st){const S=st.snaps||[];const ev=S.filter(s=>s.after!=null&&s.ok!==null);const n=ev.length,ok=ev.filter(s=>s.ok).length;
  const buckets=[[50,55],[55,60],[60,65],[65,70],[70,101]].map(([a,b])=>{const c=ev.filter(s=>{const p=s.score>0?s.prob:100-s.prob;return p>=a&&p<b;});const h=c.filter(s=>s.ok).length;return{a,b,n:c.length,hit:h,act:c.length?Math.round(h/c.length*100):null};});
  const grp=fn=>{const o={};ev.forEach(s=>{const k=fn(s);if(k==null)return;o[k]=o[k]||{n:0,ok:0};o[k].n++;if(s.ok)o[k].ok++;});return o;};
  return{v:typeof ENGINE!=='undefined'?ENGINE.v:null,n,acc:n?Math.round(ok/n*100):null,pending:S.filter(s=>s.after==null).length,neutral:S.filter(s=>s.after!=null&&s.ok===null).length,total:S.length,buckets,bySym:grp(s=>s.sym),byReg:grp(s=>s.reg),first:S.length?S[0].t:null,lastRun:st.lastRun||0,err:st.err||null,last:S.slice(-20).reverse()};}
/* ---------- v10: exchange wallet labels (seed — public labels; keep them fresh from the page settings) ---------- */
const ETH_SEED={'0x28c6c06298d514db089934071355e5743bf21d60':'Binance','0x21a31ee1afc51d94c2efccaa2092ad1028285549':'Binance','0xdfd5293d8e347dfe59e90efd55b2956a1343963d':'Binance','0x56eddb7aa87536c09ccc2793473599fd21a8b17f':'Binance','0x9696f59e4d72e237be84ffd425dcad154bf96976':'Binance','0x4976a4a02f38326660d17bf34b431dc6e2eb2327':'Binance','0xf977814e90da44bfa03b6295a0616a897441acec':'Binance','0x5a52e96bacdabb82fd05763e25335261b270efcb':'Binance','0xbe0eb53f46cd790cd13851d5eff43d12404d33e8':'Binance','0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be':'Binance','0xd551234ae421e3bcba99a0da6d736074f22192ff':'Binance','0x564286362092d8e7936f0549571a803b203aaced':'Binance','0x0681d8db095565fe8a346fa0277bffde9c0edbbf':'Binance','0xfe9e8709d3215310075d67e3ed32a380ccf451c8':'Binance','0xeb2629a2734e272bcc07bda959863f316f4bd4cf':'Binance',
 '0x71660c4005ba85c37ccec55d0c4493e66fe775d3':'Coinbase','0x503828976d22510aad0201ac7ec88293211d23da':'Coinbase','0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740':'Coinbase','0x3cd751e6b0078be393132286c442345e5dc49699':'Coinbase','0xb5d85cbf7cb3ee0d56b3bb207d5fc4b82f43f511':'Coinbase','0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43':'Coinbase','0x77696bb39917c91a0c3908d577d5e322095425ca':'Coinbase',
 '0x6cc5f688a315f3dc28a7781717a9a798a59fda7b':'OKX','0x98ec059dc3adfbdd63429454aeb0c990fba4a128':'OKX','0x5041ed759dd4afc3a72b8192c143f72f4724081a':'OKX','0x2c8fbb630289363ac80705a1a61273f76fd5a161':'OKX',
 '0x2910543af39aba0cd09dbb2d50200b3e800a63d2':'Kraken','0x0a869d79a7052c7f1b55a8ebabbea3420f0d1e13':'Kraken','0xe853c56864a2ebe4576a807d26fdc4a0ada51919':'Kraken','0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0':'Kraken','0xfa52274dd61e1643d2205169732f29114bc240b3':'Kraken','0x53d284357ec70ce289d6d64134dfac8e511c8a3d':'Kraken','0x89e51fa8ca5d66cd220baed62ed01e8951aa7c40':'Kraken','0x43984d578803891dfa9706bdeee6078d80cfc79e':'Kraken',
 '0xf89d7b9c864f589bbf53a82105107622b35eaa40':'Bybit','0xee5b5b923ffce93a870b3104b7ca09c3db80047a':'Bybit','0x1db92e2eebc8e0c075a02bea49a2935bcd2dfcf4':'Bybit',
 '0x876eabf441b2ee5b5b0554fd502a8e0600950cfa':'Bitfinex','0x77134cbc06cb00b66f4c7e623d5fdbf6777635ec':'Bitfinex','0x742d35cc6634c0532925a3b844bc454e4438f44e':'Bitfinex',
 '0xab5c66752a9e8167967685f1450532fb96d5d24f':'HTX','0xe93381fb4c4f14bda253907b18fad305d799241a':'HTX','0xfdb16996831753d5331ff813c29a93c76834a0ad':'HTX','0x46705dfff24256421a05d056c29e81bdc09723b8':'HTX','0x5c985e89dde482efe97ea9f1950ad149eb73829b':'HTX','0xdc76cd25977e0a5ae17155770273ad58648900d3':'HTX',
 '0x2b5634c42055806a59e9107ed44d43c426e58258':'KuCoin','0xd6216fc19db775df9774a6e33526131da7d19a2c':'KuCoin','0xf16e9b0d03470827a95cdfd0cb8a8a3b46969b91':'KuCoin','0x689c56aef474df92d44a1b70850f808488f9769c':'KuCoin',
 '0x0d0707963952f2fba59dd06f2b425ace40b492fe':'Gate.io','0x1c4b70a3968436b9a0a9cf5205c787eb81bb558c':'Gate.io','0xd793281182a0e3e023116004778f45c29fc14f19':'Gate.io',
 '0x0639556f03714a74a5feeaf5736a4a64ff70d206':'Bitget','0x1ab4973a48dc892cd9971ece8e01dcc7688f8f23':'Bitget',
 '0x5f65f7b609678448494de4c87521cdf6cef1e932':'Gemini','0xd24400ae8bfebb18ca49be86258a3c749cf46853':'Gemini','0x6fc82a5fe25a5cdb58bc74600a40a69c065263f8':'Gemini',
 '0x6262998ced04146fa42253a5c0af90ca02dfd2a3':'Crypto.com','0x46340b20830761efd32832a74d7169b29feb9758':'Crypto.com','0x72a53cdbbcc1b9efa39c834a540550e23463aacb':'Crypto.com',
 '0x40b38765696e3d5d8d9d834d8aad4bb6e418e489':'Robinhood','0x73af3bcf944a6559933396c1577b257e2054d935':'Robinhood'};
const TRON_SEED={'TAUN6FwrnwwmaEqYcckffC7wYmbaS6cBiX':'Binance','TQrY8tryqsYVCYS3MFbtffiPp2ccyn4STm':'Binance','TDUFoSoLyFnWTqeqXc4nYS3A4RpGBXs9sL':'Binance','TNXoiAJ3dct8Fjg4M9fkLFh9S2v9TXc32G':'Binance','TMuA6YqfCeX8EhbfYEg5y7S4DqzSJireY9':'Binance','TWd4WrZ9wn84f5x1hZhL4DHvk738ns5jwb':'Binance','TZ2JFZX2Jn3ad3imNz1kjVprM7v8M3gxwj':'OKX','TN4rBwJdwo4t9n6WJ7KvC3NX7R6BvQvkJm':'Bybit','TU4vEruvZwLLkSfV9bNw12EJTPvNr7Pvaa':'HTX','TJ9aSbUNtBo4MPgeaV3GYRzsM11pvZhxV2':'HTX','TWw13TFU9EhRwQtJLd4Bf6SXZhRXdXBb4v':'KuCoin'};
const USDT_ETH='0xdac17f958d2ee523a2206206994597c13d831ec7',USDC_ETH='0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',USDT_TRON='TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t';
const ETH_HOT_FOR_NATIVE=['0x28c6c06298d514db089934071355e5743bf21d60','0x21a31ee1afc51d94c2efccaa2092ad1028285549','0xdfd5293d8e347dfe59e90efd55b2956a1343963d','0x71660c4005ba85c37ccec55d0c4493e66fe775d3','0x6cc5f688a315f3dc28a7781717a9a798a59fda7b','0x2910543af39aba0cd09dbb2d50200b3e800a63d2','0xf89d7b9c864f589bbf53a82105107622b35eaa40','0x2b5634c42055806a59e9107ed44d43c426e58258'];
async function wallets(env){const w=await kvGet(env,'wallets',null);return{eth:w&&w.eth&&Object.keys(w.eth).length?w.eth:ETH_SEED,tron:w&&w.tron&&Object.keys(w.tron).length?w.tron:TRON_SEED};}
function classify(fromL,toL,isStable){if(fromL&&toL)return{type:'exchange',label:`${fromL} → ${toL}`};if(toL)return{type:isStable?'stableIn':'cryptoIn',label:`→ ${toL}`};if(fromL)return{type:isStable?'stableOut':'cryptoOut',label:`${fromL} →`};return null;}
async function chainCron(env){
  const W=await wallets(env),minUsd=+(env.CHAIN_MIN||1e6),alertUsd=+(env.CHAIN_ALERT||25e6),st=await kvGet(env,'chain',{ev:[],lastRun:0,seen:[]});
  const seen=new Set(st.seen||[]);const now=Date.now();const add=[];const push=e=>{if(seen.has(e.id))return;seen.add(e.id);add.push(e);};
  let ethPx=0;try{const r=await spotGet('/api/v3/ticker/price?symbol=ETHUSDT');ethPx=+r.price;}catch(e){}
  if(env.ES_KEY){
    const ES=(q)=>getJ(`https://api.etherscan.io/v2/api?chainid=1&apikey=${env.ES_KEY}&${q}`);
    for(const [tok,addr,off] of [['USDT',USDT_ETH,300],['USDC',USDC_ETH,200]]){try{const r=await ES(`module=account&action=tokentx&contractaddress=${addr}&page=1&offset=${off}&sort=desc`);if(r.status==='1'&&Array.isArray(r.result))for(const x of r.result){const usd=+x.value/Math.pow(10,+x.tokenDecimal||6);if(usd<minUsd)continue;const c=classify(W.eth[x.from.toLowerCase()],W.eth[x.to.toLowerCase()],true);if(!c)continue;push({id:x.hash+':'+tok,t:+x.timeStamp*1000,chain:'ETH',tok,usd,amt:usd,type:c.type,label:c.label,from:x.from,to:x.to});}}catch(e){}}
    if(ethPx){for(const a of ETH_HOT_FOR_NATIVE){if(!W.eth[a])continue;try{const r=await ES(`module=account&action=txlist&address=${a}&page=1&offset=40&sort=desc`);if(r.status==='1'&&Array.isArray(r.result))for(const x of r.result){const eth=+x.value/1e18,usd=eth*ethPx;if(usd<minUsd||x.isError==='1')continue;const c=classify(W.eth[x.from.toLowerCase()],W.eth[x.to.toLowerCase()],false);if(!c)continue;push({id:x.hash+':ETH',t:+x.timeStamp*1000,chain:'ETH',tok:'ETH',usd,amt:eth,type:c.type,label:c.label,from:x.from,to:x.to});}}catch(e){}}}
  }
  const tronAddrs=Object.keys(W.tron).slice(0,10);const since=Math.max(st.lastRun||0,now-2*3600e3);
  for(const a of tronAddrs){try{const r=await fetch(`https://api.trongrid.io/v1/accounts/${a}/transactions/trc20?only_confirmed=true&limit=200&contract_address=${USDT_TRON}&min_timestamp=${since}`,{headers:Object.assign({'accept':'application/json'},env.TRON_KEY?{'TRON-PRO-API-KEY':env.TRON_KEY}:{})});if(!r.ok)continue;const j=await r.json();
    for(const x of (j.data||[])){if(x.type&&x.type!=='Transfer')continue;const usd=+x.value/Math.pow(10,+(x.token_info&&x.token_info.decimals)||6);if(usd<minUsd)continue;const c=classify(W.tron[x.from],W.tron[x.to],true);if(!c)continue;push({id:x.transaction_id+':T',t:+x.block_timestamp,chain:'TRON',tok:'USDT',usd,amt:usd,type:c.type,label:c.label,from:x.from,to:x.to});}}catch(e){}}
  const cut=now-48*3600e3;const ev=add.concat(st.ev||[]).filter(e=>e.t>cut).sort((a,b)=>b.t-a.t).slice(0,400);
  const nst={ev,lastRun:now,seen:[...seen].slice(-1500),src:{etherscan:!!env.ES_KEY,tron:tronAddrs.length,eth:ethPx}};
  await kvPut(env,'chain',nst);
  for(const e of add.filter(x=>x.usd>=alertUsd&&now-x.t<3600e3).slice(0,5))await tg(env,`⛓ <b>حركة on-chain كبيرة</b> ${e.type==='stableIn'?'🟢 ستيبل داخل عالبورصة (قوّة شراء)':e.type==='stableOut'?'🟡 ستيبل طالع من البورصة':e.type==='cryptoIn'?'🔴 كريبتو داخل عالبورصة (ضغط بيع)':e.type==='cryptoOut'?'🟢 كريبتو طالع من البورصة (تجميع)':'بين بورصتين'}\n${usd(e.usd)} ${e.tok} ${e.label} · ${e.chain}`);
  return{added:add.length,total:ev.length};
}
function chainAgg(ev){const cut=Date.now()-86400e3;const a={stableIn:0,stableOut:0,cryptoIn:0,cryptoOut:0,exchange:0,n:0};for(const e of ev){if(e.t<cut)continue;a[e.type]=(a[e.type]||0)+e.usd;a.n++;}return a;}
/* ---------- v10: scenario watcher (lite: close vs level + volume + delta) ---------- */
async function condCron(env,st){
  const conds=await kvGet(env,'conds',[]);if(!conds.length)return[];const keep=[],trig=await kvGet(env,'triggered',[]),msgs=[];const now=Date.now();
  for(const c of conds.slice(0,10)){try{const kl=await spotGet(`/api/v3/klines?symbol=${c.pair}&interval=${c.tf}&limit=52`);const closed=kl.filter(k=>+k[6]<now);const avg=closed.slice(0,-1).reduce((a,k)=>a+ +k[5],0)/Math.max(1,closed.length-1);
      const fresh=closed.filter(k=>+k[0]>(c.lastCheck||c.created||0));let hit=null;
      const E=(typeof ENGINE!=='undefined')?ENGINE:null;const KN=(E&&E.SCEN_KIND&&E.SCEN_KIND[c.kind])||c.kind;const parsed=closed.map(k=>({open:+k[1],high:+k[2],low:+k[3],close:+k[4],vol:+k[5],delta:2*(+k[9])-(+k[5]),ts:+k[0],T:+k[6]}));const atr=E?(E.fAtr(parsed.map(x=>({high:x.high,low:x.low,close:x.close})))||0):0;
      for(const k of fresh){const close=+k[4],high=+k[2],low=+k[3],vol=+k[5],delta=2*(+k[9])-vol,L=c.level,rng=high-low;let ok=false;
        if(E&&E.fCondHit){const idx=parsed.findIndex(x=>x.ts===+k[0]);const r=E.fCondHit(c.kind,L,parsed[idx]||{open:+k[1],high,low,close,vol,delta},parsed[idx-1]||null,avg,atr||rng,c.st||{});c.st=r.st;ok=r.hit;if(ok&&r.sl!=null)c.slHit=r.sl;}
        else ok=c.kind==='breakUp'?close>L&&vol>=1.5*avg&&delta>0:c.kind==='breakDown'?close<L&&vol>=1.5*avg&&delta<0:c.kind==='rejectUp'?high>=L&&close<L&&delta<0&&(high-close)>=0.3*rng:c.kind==='bounceDown'?low<=L&&close>L&&delta>0&&(close-low)>=0.3*rng:false;
        if(ok){hit={price:close,t:+k[6]};break;}}
      c.lastCheck=closed.length?+closed[closed.length-1][0]:c.lastCheck;c.bars=(c.bars||0)+fresh.length;
      if(hit){trig.push({id:c.id,pair:c.pair,tf:c.tf,kind:c.kind,level:c.level,price:hit.price,t:hit.t,sl:c.slHit!=null?c.slHit:undefined});msgs.push(`🌳 <b>تحقّق سيناريو ${c.pair.replace('USDT','')}</b> ${KN} @ ${fp(c.level)} — إغلاق ${fp(hit.price)} (${c.tf})${c.slHit!=null?' · stop '+fp(c.slHit):''} · افتح الصفحة لتتابع الصفقة`);}
      else if(c.bars>=(c.maxBars||48)){msgs.push(`🌳 سيناريو ${c.pair.replace('USDT','')} ${KN} @ ${fp(c.level)} انتهى بلا تحقّق`);}
      else keep.push(c);}catch(e){keep.push(c);}}
  await kvPut(env,'conds',keep);if(trig.length)await kvPut(env,'triggered',trig.slice(-50));
  return msgs;
}

export default{
  async fetch(req,env){
    if(req.method==='OPTIONS')return new Response(null,{headers:CORS});
    const url=new URL(req.url),p=url.pathname;
    try{
      if(p==='/health'){const st=await kvGet(env,'cron',{});return json({ok:true,v:'11.0',engine:typeof ENGINE!=='undefined'?ENGINE.v:null,symbols:symbols(env),lastCron:st.lastRun||0,telegram:!!(env.TG_TOKEN&&env.TG_CHAT),gateway:['/news','/fng','/gold','/cg','/macro','/multi','/options','/chain','/forward']});}
      if(p==='/multi'){const sym=(url.searchParams.get('symbol')||'BTCUSDT').toUpperCase().replace(/[^A-Z0-9]/g,'');return json(await cached('multi:'+sym,60,()=>multi(sym)));}
      if(p==='/options'){const cur=(url.searchParams.get('cur')||'BTC').toUpperCase().replace(/[^A-Z]/g,'');if(!['BTC','ETH'].includes(cur))return json({error:'BTC/ETH only'},400);return json(await cached('opt:'+cur,300,()=>options(cur)));}
      if(p==='/macro')return json(await cached('macro',600,macro));
      if(p==='/forward'){const st=await kvGet(env,'fwd',{snaps:[]});return json(fwdStats(st));}
      if(p==='/fwdrun'){if(!okKey(env,url.searchParams.get('key')))return json({error:'key'},403);return json(await forwardStep(env));}
      if(p==='/news'){const coin=(url.searchParams.get('coin')||'').toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,12);return json(await cached('news:'+coin,300,()=>newsAll(coin)));}
      if(p==='/fng')return json(await cached('fng',600,()=>getJ('https://api.alternative.me/fng/?limit=31&format=json')));
      if(p==='/gold')return json(await cached('gold',120,()=>getJ('https://api.gold-api.com/price/XAU')));
      if(p==='/cg')return json(await cached('cg',300,()=>cgAll(env)));
      if(p==='/state'){const sym=(url.searchParams.get('symbol')||'').toUpperCase();const all=await kvGet(env,'agg',{});const alerts=await kvGet(env,'alerts',[]);const triggered=await kvGet(env,'triggered',[]);return json({agg24:agg24(all[sym]||[]),alerts,lastPush:all._lastPush||0,triggered});}
      if(p==='/chain'){const st=await kvGet(env,'chain',{ev:[],lastRun:0});const W=await wallets(env);return json({ev:(st.ev||[]).slice(0,150),agg24:chainAgg(st.ev||[]),lastRun:st.lastRun||0,src:st.src||{},wallets:{eth:Object.keys(W.eth).length,tron:Object.keys(W.tron).length},minUsd:+(env.CHAIN_MIN||1e6)});}
      if(p==='/run'){if(!okKey(env,url.searchParams.get('key')))return json({error:'key'},403);return json(await cron(env));}
      if(req.method==='POST'){const body=await req.json().catch(()=>({}));if(!okKey(env,body.key))return json({error:'key'},403);
        if(p==='/push')return handlePush(env,body);
        if(p==='/alerts'){const al=(body.alerts||[]).slice(0,50).map(a=>({pair:String(a.pair||'').toUpperCase(),dir:a.dir==='below'?'below':'above',price:+a.price,fired:a.fired||0})).filter(a=>a.pair&&a.price>0);await kvPut(env,'alerts',al);return json({ok:true,n:al.length});}
        if(p==='/telegram'){const ok=await tg(env,String(body.text||'').slice(0,3500));return json({ok});}
        if(p==='/weights'){const W={};for(const [k,v] of Object.entries(body.W||{}))if(isFinite(+v)&&+v>=0)W[k]=+v;await kvPut(env,'weights',W);return json({ok:true,n:Object.keys(W).length});}
        if(p==='/fwdreset'){await kvPut(env,'fwd',{snaps:[],i:0,lastRun:0});return json({ok:true});}
        if(p==='/wallets'){const eth={},tron={};for(const [a,l] of Object.entries(body.eth||{}))if(/^0x[0-9a-f]{40}$/i.test(a)&&l)eth[a.toLowerCase()]=String(l).slice(0,24);for(const [a,l] of Object.entries(body.tron||{}))if(/^T[1-9A-HJ-NP-Za-km-z]{33}$/.test(a)&&l)tron[a]=String(l).slice(0,24);await kvPut(env,'wallets',{eth,tron});return json({ok:true,eth:Object.keys(eth).length,tron:Object.keys(tron).length});}
        if(p==='/cond'){const old=await kvGet(env,'conds',[]);const byId=Object.fromEntries(old.map(c=>[c.id,c]));const conds=(body.conds||[]).slice(0,10).map(c=>Object.assign({id:String(c.id),pair:String(c.pair||'').toUpperCase(),tf:String(c.tf||'15m'),kind:String(c.kind||''),level:+c.level,created:+c.created||Date.now(),maxBars:+c.maxBars||48},byId[String(c.id)]?{lastCheck:byId[String(c.id)].lastCheck,bars:byId[String(c.id)].bars}:{})).filter(c=>c.pair&&c.level>0&&['breakUp','breakDown','rejectUp','bounceDown'].includes(c.kind));await kvPut(env,'conds',conds);return json({ok:true,n:conds.length});}
        if(p==='/ack'){const trig=await kvGet(env,'triggered',[]);const ids=new Set((body.ids||[]).map(String));await kvPut(env,'triggered',trig.filter(t=>!ids.has(String(t.id))));return json({ok:true});}
      }
      return json({error:'not found'},404);
    }catch(e){return json({error:String(e.message||e)},500);}
  },
  async scheduled(ev,env,ctx){ctx.waitUntil(ev.cron&&ev.cron.startsWith('*/10')?chainCron(env):ev.cron&&ev.cron.startsWith('2-57/5')?forwardStep(env):cron(env));}
};

/* ===== ENGINE v11.3 — auto-extracted from index.html (FAM Flow) on 2026-11-03T13:50:54.251Z — don't edit here: edit the page and re-download ===== */
const ENGINE=(()=>{
const W_DEFAULT={"delta":22,"mom":10,"trend":15,"div":15,"oi":15,"flow":12,"fund":10,"sf":10,"taker":8,"ob":6,"top":6,"liq":5,"news":5,"fng":4,"prem":6,"xfund":6,"rot":6,"chain":6,"whale":6,"macro":4};const W_NAME={"delta":"ضغط الـ market orders (Delta 10 شمعات)","mom":"الزخم (حركة آخر 3 شموع × ATR + delta)","trend":"الترند والهيكل (EMA + MTF)","div":"Divergence سعر/Delta","oi":"OI مع السعر","flow":"تدفّق السيولة 1h/4h (Spot+Fut)","fund":"Funding rate","sf":"Spot vs Futures (تدفّق 24h)","taker":"Taker Buy/Sell (Futures)","ob":"Order book imbalance","top":"كبار المتداولين vs العامة","liq":"Liquidations (session)","news":"نبرة الأخبار (آخر 6 ساعات)","fng":"Fear & Greed","prem":"Coinbase premium (طلب أميركي)","xfund":"Funding عبر البورصات (مرجّح بالـ OI)","rot":"التدوير — المال داخل على هالعملة نسبةً للسوق","chain":"تدفّقات البورصات on-chain (ستيبل داخل vs كريبتو داخل)","whale":"الحيتان — صفقات كبيرة آخر ساعة (شراء vs بيع)","macro":"مزاج الماكرو (الدولار، العوائد، VIX، S&P، النفط) — على 4h/1d والذهب بس"};const activeCal=()=>null;const EV_PRE_H=2,EV_POST_H=0.5;const SCEN_KIND={"breakUp":"كسر للفوق","breakDown":"كسر للتحت","rejectUp":"رفض مقاومة","bounceDown":"ارتداد من دعم","sweepLow":"كسر كاذب (سيولة) ↑","sweepHigh":"كسر كاذب (سيولة) ↓","retestUp":"كسر + إعادة اختبار ↑","retestDown":"كسر + إعادة اختبار ↓"};
const sign=n=>n>0?'+':'';
const clamp=(v,a=-1,b=1)=>Math.max(a,Math.min(b,v));
const fmt=(n,d=2)=>n==null||isNaN(n)?'—':Number(n).toLocaleString('en-US',{maximumFractionDigits:d});
const fp=n=>fmt(n,n<1?5:n<100?3:2);
const usd=n=>n==null||isNaN(n)?'—':(Math.abs(n)>=1e9?'$'+(n/1e9).toFixed(2)+'B':Math.abs(n)>=1e6?'$'+(n/1e6).toFixed(2)+'M':Math.abs(n)>=1e3?'$'+(n/1e3).toFixed(1)+'K':'$'+n.toFixed(0));
function parseK(k){return{time:k[0]/1000,open:+k[1],high:+k[2],low:+k[3],close:+k[4],vol:+k[5],delta:2*(+k[9])-(+k[5]),ts:k[0],qv:+k[7],tbq:+k[10]};}
function ema(arr,n){let k=2/(n+1),e=arr[0];for(let i=1;i<arr.length;i++)e=arr[i]*k+e*(1-k);return e;}
function fRegime(cs){
  if(cs.length<40)return null;
  const cl=cs.slice(-30).map(c=>c.close),net=Math.abs(cl[cl.length-1]-cl[0]);let sum=0;
  for(let i=1;i<cl.length;i++)sum+=Math.abs(cl[i]-cl[i-1]);
  const er=sum?net/sum:0,dir=cl[cl.length-1]>cl[0]?'صاعد':'هابط';
  if(er>0.35)return{type:'trend',label:'ترند '+dir,er,dir};if(er<0.2)return{type:'range',label:'Range (تذبذب)',er,dir};return{type:'mixed',label:'انتقالي',er,dir};
}
function fAtr(cs,n=14){if(cs.length<n+1)return 0;let s=0;for(let i=cs.length-n;i<cs.length;i++){const c=cs[i],pc=cs[i-1].close;s+=Math.max(c.high-c.low,Math.abs(c.high-pc),Math.abs(c.low-pc));}return s/n;}
function fSwing(cs,n=20){const w=cs.slice(-n);let lo=Infinity,hi=-Infinity;for(const c of w){if(c.low<lo)lo=c.low;if(c.high>hi)hi=c.high;}return{low:lo,high:hi};}
function fPivots(cs,n=5,span=120){
  const w=cs.slice(-span);const res=[];
  for(let i=n;i<w.length-n;i++){const h=w[i].high,l=w[i].low;let ph=true,pl=true;for(let j=i-n;j<=i+n;j++){if(j===i)continue;if(w[j].high>=h)ph=false;if(w[j].low<=l)pl=false;}
    if(ph)res.push({p:h,type:'high',i});if(pl)res.push({p:l,type:'low',i});}
  res.forEach(a=>{a.eq=res.some(b=>b!==a&&b.type===a.type&&Math.abs(a.p-b.p)/a.p<0.0015);});
  return res;
}
function fStructure(cs){
  if(cs.length<60)return null;
  const pv=fPivots(cs,4,100),hs=pv.filter(v=>v.type==='high').slice(-2),ls=pv.filter(v=>v.type==='low').slice(-2);
  if(hs.length<2||ls.length<2)return null;
  const hh=hs[1].p>hs[0].p,hl=ls[1].p>ls[0].p,last=cs[cs.length-1].close;
  /* v11.3: الكسر الفوري — إغلاق فوق آخر قمة مؤكّدة بيقلب الهيكل فوراً (ما ننطر 4 شموع تأكيد pivot)، والعكس تحت آخر قاع */
  if(!hh&&last>hs[1].p)return{s:0.5,label:'انقلاب صاعد (كسر آخر قمة '+fp(hs[1].p)+' — بانتظار قاع أعلى)',flip:'up'};
  if(hl&&last<ls[1].p)return{s:-0.5,label:'انقلاب هابط (كسر آخر قاع '+fp(ls[1].p)+' — بانتظار قمة أدنى)',flip:'down'};
  if(hh&&hl)return{s:1,label:'صاعد (HH/HL)'};if(!hh&&!hl)return{s:-1,label:'هابط (LH/LL)'};
  return{s:0,label:hh?'مختلط (HH/LL — توسّع)':'مختلط (LH/HL — انضغاط)'};
}
function fShock(cs){if(cs.length<20)return null;const a=fAtr(cs);if(!a)return null;const mx=Math.max(...cs.slice(-2).map(c=>c.high-c.low));return mx>3*a?{mult:mx/a}:null;}
function fThrust(cs){
  if(cs.length<30)return null;const a=fAtr(cs);if(!a)return null;const n=cs.length;
  for(const [k,th] of [[3,2.5],[5,3.5]]){const c0=cs[n-1-k],c1=cs[n-1];if(!c0)continue;const move=c1.close-c0.close,mult=Math.abs(move)/a;if(mult<th)continue;
    const last=cs.slice(-k),agree=last.filter(c=>Math.sign(c.close-c.open)===Math.sign(move)).length,dsum=last.reduce((x,c)=>x+c.delta,0);
    if(agree*2>k)return{dir:move>0?'up':'down',mult,k,delta:dsum,agree};}
  return null;
}
function fAgg(cs,k){const out=[];for(let end=cs.length;end>0;end-=k){const g=cs.slice(Math.max(0,end-k),end);let h=-Infinity,l=Infinity,v=0,d=0,qv=0,tbq=0;for(const c of g){if(c.high>h)h=c.high;if(c.low<l)l=c.low;v+=c.vol;d+=c.delta;qv+=c.qv||0;tbq+=c.tbq||0;}out.unshift({open:g[0].open,high:h,low:l,close:g[g.length-1].close,vol:v,delta:d,ts:g[0].ts,qv,tbq});}return out;}
function fMini(cs){const closes=cs.map(c=>c.close),e20=ema(closes,20),p=closes[closes.length-1];const d10=cs.slice(-10).reduce((a,c)=>a+c.delta,0),v10=cs.slice(-10).reduce((a,c)=>a+c.vol,0)||1;const s=clamp(d10/v10*4)*0.6+clamp((p-e20)/e20*40)*0.4;return{s,label:s>0.2?'صعودي':s<-0.2?'هبوطي':'محايد',p};}
function fTrend(cs,mtf){
  if(cs.length<60)return null;
  const closes=cs.map(c=>c.close),p=closes[closes.length-1];
  const e20=ema(closes,20),e50=ema(closes,50),e200=closes.length>=200?ema(closes,200):null;
  let s=0;const parts=[];
  s+=p>e20?0.25:-0.25;s+=e20>e50?0.25:-0.25;if(e200!=null)s+=p>e200?0.15:-0.15;
  parts.push(`السعر ${p>e20?'فوق':'تحت'} EMA20، EMA20 ${e20>e50?'فوق':'تحت'} EMA50`+(e200!=null?`، ${p>e200?'فوق':'تحت'} EMA200`:''));
  const h1=mtf&&mtf['1h'],h4=mtf&&mtf['4h'];
  if(h1)s+=clamp(h1.s*0.3,-0.2,0.2);if(h4)s+=clamp(h4.s*0.4,-0.25,0.25);
  if(h1&&h4)parts.push(`1h ${h1.label} / 4h ${h4.label}`);
  const st=fStructure(cs);if(st){s+=st.s*0.15;parts.push('هيكل '+st.label);}
  return{s:clamp(s),exp:parts.join(' — ')};
}
function fOIState(oiArr,cs){
  if(!oiArr||oiArr.length<2||cs.length<5)return null;
  const nowT=oiArr[oiArr.length-1].t||0;let old=oiArr[0];
  if(nowT){for(let i=oiArr.length-1;i>=0;i--){if(oiArr[i].t<=nowT-3600e3+1){old=oiArr[i];break;}}}else old=oiArr[Math.max(0,oiArr.length-5)];
  const oiNow=oiArr[oiArr.length-1].oi,oiCh=old.oi?(oiNow-old.oi)/old.oi:0;
  const pNow=cs[cs.length-1].close,ago=cs[cs.length-1].ts-3600e3;let pOld=pNow;
  for(let i=cs.length-1;i>=0;i--){if(cs[i].ts<=ago){pOld=cs[i].close;break;}}
  const pCh=(pNow-pOld)/pOld;let s,label,exp;
  if(pCh>0.001&&oiCh>0.003){s=1;label='سعر↑ OI↑';exp='longs جدد بمال جديد — حركة صحّية، بس راقب الـ funding';}
  else if(pCh>0.001&&oiCh<-0.003){s=-0.5;label='سعر↑ OI↓';exp='shorts عم يتغطّوا (squeeze) — صعود بلا مال جديد، ضعيف';}
  else if(pCh<-0.001&&oiCh>0.003){s=-1;label='سعر↓ OI↑';exp='shorts جدد بقناعة — ضغط هبوطي حقيقي';}
  else if(pCh<-0.001&&oiCh<-0.003){s=0.5;label='سعر↓ OI↓';exp='longs عم ينقفلوا/تصفيات — الرافعة عم تنظّف، قرب نهاية الدفعة';}
  else{s=0;label='مستقر';exp='ما في تغيّر مهم بالـ OI مع السعر';}
  return{s,label,exp,oiCh,pCh};
}
function fFlowFactor(fl){
  if(!fl||!fl.s1||!fl.s4)return null;const r=x=>x&&x.vol?x.net/x.vol:0;
  let s=(r(fl.s1)*0.35+r(fl.s4)*0.25)*4;if(fl.f1&&fl.f4)s+=(r(fl.f1)*0.25+r(fl.f4)*0.15)*4;s=clamp(s);
  const exp=`Spot 1h ${sign(fl.s1.net)}${usd(fl.s1.net)} / 4h ${sign(fl.s4.net)}${usd(fl.s4.net)}`+(fl.f4?` — Futures 4h ${sign(fl.f4.net)}${usd(fl.f4.net)}`:'')+(fl.rvol?` — الحجم ${fl.rvol.toFixed(1)}× المعدّل`:'');
  return{s,exp};
}
function fFactors(ctx){
  const cs=ctx.candles,p=cs[cs.length-1].close,Wt=ctx.W||W_DEFAULT,out=[];
  const add=(key,s,exp,extra)=>{s=clamp(s);const w=Wt[key]||0;out.push(Object.assign({key,name:W_NAME[key],w,s,exp,c:s*w},extra||{}));};
  const last10=cs.slice(-10),d10=last10.reduce((a,c)=>a+c.delta,0),v10=last10.reduce((a,c)=>a+c.vol,0)||1,dr=d10/v10;
  add('delta',dr*4,dr>0.05?'المشترين عم يضربوا market buys أكتر بوضوح':dr<-0.05?'البياعين عم يضربوا market sells أكتر بوضوح':'شبه متوازن، ما في طرف مسيطر',{d10,dr});
  const p10=cs[cs.length-11].close,pch=(p-p10)/p10;let divS=0,divT='ما في';
  if(pch>0.002&&dr<-0.03){divS=-1;divT='سعر طالع + delta سلبي = صعود بلا شراء (فخ محتمل)';}
  else if(pch<-0.002&&dr>0.03){divS=1;divT='سعر نازل + delta إيجابي = مشتري كبير عم يمتصّ البيع';}
  else if(pch>0.002&&dr>0.03){divS=0.4;divT='سعر ودلتا سوا للفوق — تأكيد';}
  else if(pch<-0.002&&dr<-0.03){divS=-0.4;divT='سعر ودلتا سوا للتحت — تأكيد';}
  add('div',divS,divT,{divS});
  /* v11.3: الزخم — حركة آخر 3 شموع بالـ ATR مع اتجاه الـ delta: بيعادل العوامل البطيئة (EMA/الهيكل) لمّا السوق يتحرّك عمودي */
  {const aM=fAtr(cs)||p*0.005,c3=cs[cs.length-4],l3=cs.slice(-3);if(c3){const mv=(p-c3.close)/aM,d3=l3.reduce((x,c)=>x+c.delta,0),v3=l3.reduce((x,c)=>x+c.vol,0)||1,dr3=d3/v3,agree=Math.sign(mv)===Math.sign(dr3)&&Math.abs(dr3)>0.03;let sM=clamp(mv/3);if(!agree)sM*=0.5;
    add('mom',sM,Math.abs(mv)<0.8?'ما في زخم واضح بآخر 3 شموع':`${mv>0?'اندفاع صاعد':'اندفاع هابط'} ${Math.abs(mv).toFixed(1)}×ATR بآخر 3 شموع ${agree?'مع delta موافق — زخم حقيقي':'بس الـ delta مش موافق — زخم بلا تأكيد'}`,{mv,dr3});}}
  const tr=fTrend(cs,ctx.mtf||{});if(tr)add('trend',tr.s,tr.exp);
  const nf=fFlowFactor(ctx.flow);if(nf)add('flow',nf.s,nf.exp);
  if(ctx.ob&&(ctx.ob.bid+ctx.ob.ask)>0){const t=ctx.ob.bid+ctx.ob.ask,imb=(ctx.ob.bid-ctx.ob.ask)/t;add('ob',imb*3,imb>0.15?'طلبات الشراء أكبر قرب السعر':imb<-0.15?'عروض البيع أكبر قرب السعر':'الدفتر متوازن');}
  const st=ctx.oiState;if(st)add('oi',st.s,st.exp,{label:st.label});
  if(ctx.funding!=null){const f=ctx.funding*100,s=f>0.05?-1:f>0.02?-0.4:f<-0.02?0.6:0;add('fund',s,f>0.05?'funding عالي جداً — longs مزدحمين، خطر تصفية للتحت':f>0.02?'funding مرتفع شوي — حماس longs':f<-0.02?'funding سلبي — shorts مزدحمين، ممكن squeeze للفوق':'funding طبيعي');}
  if(ctx.taker!=null)add('taker',(ctx.taker-1)*4,ctx.taker>1.1?'شراء عدواني على الفيوتشر':ctx.taker<0.9?'بيع عدواني على الفيوتشر':'متوازن');
  if(ctx.topLsr!=null&&ctx.lsr!=null){const s=(ctx.topLsr-ctx.lsr)*0.8;add('top',s,ctx.topLsr>ctx.lsr+0.3?'الكبار أكتر long من العامة — الكبار عادةً أصح':ctx.topLsr<ctx.lsr-0.3?'الكبار أقل long من العامة — العامة متفائلين زيادة':'مش مختلفين كتير');}
  if(ctx.sf&&ctx.sf.spot!=null&&ctx.sf.fut!=null){const sp=ctx.sf.spot,fu=ctx.sf.fut;let s=0,txt='—';
    if(sp>0&&fu>0){s=0.8;txt='Spot + Futures شراء';}else if(sp<0&&fu<0){s=-0.8;txt='Spot + Futures بيع';}
    else if(sp>0&&fu<0){s=0.3;txt='Spot شراء / Futures بيع (مال حقيقي أقوى)';}else if(sp<0&&fu>0){s=-0.3;txt='Spot بيع / Futures شراء (رافعة بلا دعم)';}
    add('sf',s,txt+(ctx.sf.src==='24h'?' على 24h':' (session)')+' — الشراء بالـ spot مال حقيقي، بالـ futures رافعة ممكن تتصفّى',{txt});}
  if(ctx.liq&&ctx.liq.long+ctx.liq.short>0){const L=ctx.liq,r=(L.short-L.long)/(L.long+L.short);add('liq',r,L.long>L.short*2?'الـ longs عم ينضربوا — الهبوط عم يتغذّى على التصفيات':L.short>L.long*2?'الـ shorts عم ينضربوا — squeeze للفوق':'تصفيات متوازنة');}
  if(ctx.newsTone!=null){const nt=ctx.newsTone;add('news',nt,(ctx.goldNews?'(أخبار الذهب/macro آخر 6 ساعات) ':'')+(Math.abs(nt)<0.15?'أخبار محايدة/مختلطة':nt>0?(ctx.goldNews?'عناوين إيجابية للذهب (خفض فايدة/دولار ضعيف/توتّر…)':'عناوين إيجابية مسيطرة (ETF/تبنّي/ارتفاع…)'):(ctx.goldNews?'عناوين سلبية للذهب (رفع فايدة/دولار قوي…)':'عناوين سلبية مسيطرة (hack/تنظيم/هبوط…)')));}
  if(ctx.fng!=null){const v=ctx.fng,s=v<=20?0.7:v<=30?0.3:v>=80?-0.7:v>=70?-0.3:0;add('fng',s,v<=20?'خوف شديد — الكل باع، contrarian صعودي':v>=80?'جشع شديد — الكل مشتري، خطر تصحيح':v<=30?'خوف — ميل contrarian خفيف للفوق':v>=70?'جشع — حذر':'مزاج معتدل، ما بيأثر',{name:W_NAME.fng+' ('+v+(ctx.fngCls?' '+ctx.fngCls:'')+')'});}
  if(ctx.prem!=null){const pr=ctx.prem,s=pr>0.08?0.8:pr>0.03?0.4:pr<-0.08?-0.8:pr<-0.03?-0.4:0;add('prem',s,pr>0.03?`Coinbase أغلى من Binance بـ ${pr.toFixed(3)}% — شراء أميركي/مؤسساتي`:pr<-0.03?`Coinbase أرخص بـ ${Math.abs(pr).toFixed(3)}% — بيع أميركي`:'ما في premium واضح');}
  if(ctx.xfund!=null){const f=ctx.xfund,s=f>0.05?-1:f>0.02?-0.4:f<-0.02?0.6:0;add('xfund',s,f>0.05?`Funding مرجّح ${f.toFixed(4)}% على كل البورصات — longs مزدحمين بكل السوق`:f>0.02?'funding مرتفع شوي عبر البورصات':f<-0.02?'funding سلبي عبر البورصات — shorts مزدحمين، squeeze ممكن':'funding طبيعي عبر البورصات');}
  if(ctx.rot!=null){const r=ctx.rot;add('rot',clamp(r.z/2),r.z>0.8?`المال داخل على ${r.base} أكتر من باقي السوق (ترتيب ${r.rank}/${r.n}) — ${r.verdict}`:r.z<-0.8?`المال طالع من ${r.base} نسبةً للسوق (ترتيب ${r.rank}/${r.n}) — ${r.verdict}`:`تدفّق ${r.base} عادي نسبةً للسوق — ${r.verdict}`);}
  if(ctx.chain!=null){const c=ctx.chain;add('chain',c.s,c.s>0.3?`ستيبل كوين داخل عالبورصات ${usd(c.stableIn)} مقابل كريبتو داخل ${usd(c.cryptoIn)} (24h) — قوّة شراء قادمة`:c.s<-0.3?`كريبتو داخل عالبورصات ${usd(c.cryptoIn)} مقابل ستيبل ${usd(c.stableIn)} — ضغط بيع محتمل`:`تدفّقات البورصات متوازنة (ستيبل ${usd(c.stableIn)} / كريبتو ${usd(c.cryptoIn)} داخل)`);}
  if(ctx.macro&&ctx.macro.s!=null)add('macro',ctx.macro.s,ctx.macro.exp,{label:ctx.macro.label});
  if(ctx.whale&&ctx.whale.buy+ctx.whale.sell>=2*(ctx.whale.min||100000)){const w=ctx.whale,t=w.buy+w.sell,imb=(w.buy-w.sell)/t,mag=Math.min(1,t/(10*(w.min||100000)));
    add('whale',imb*mag*1.2,imb>0.25?`الحيتان عم يشتروا آخر ساعة: ${usd(w.buy)} شراء مقابل ${usd(w.sell)} بيع (${w.n} صفقة فوق ${usd(w.min||100000)})`:imb<-0.25?`الحيتان عم يبيعوا آخر ساعة: ${usd(w.sell)} بيع مقابل ${usd(w.buy)} شراء (${w.n} صفقة)`:`الحيتان متوازنين آخر ساعة (${usd(t)} على ${w.n} صفقة)`,{buy:w.buy,sell:w.sell});}
  return out;
}
function fConf(m){let conf=1;const reg=m.regime;if(reg&&reg.type==='range')conf=0.6;if(reg&&reg.type==='mixed')conf=0.8;if(m.evHigh)conf*=0.5;if(m.evMed)conf*=0.85;if(m.hot)conf*=0.9;if(m.shock)conf*=0.7;if(m.missing&&m.missing.length)conf*=Math.pow(0.93,m.missing.length);return conf;}
function fBias(factors,conf,cal){const wsum=factors.reduce((a,f)=>a+f.w,0),score=factors.reduce((a,f)=>a+f.c,0);const raw0=wsum?score/wsum:0,raw=raw0*conf;const prob0=Math.round(50+raw*35);let prob=prob0,calibrated=false;
  const c=cal===undefined?activeCal():cal;
  if(c&&Math.abs(raw)>=0.12){const dp=raw>0?prob0:100-prob0;const b=c.map.find(x=>dp>=x.a&&dp<x.b&&x.n>=15&&x.act!=null);if(b){const dpc=Math.round(Math.max(50,Math.min(95,b.act)));prob=raw>0?dpc:100-dpc;calibrated=true;}}
  const label=raw>0.35?'ميل صعودي قوي':raw>0.12?'ميل صعودي':raw<-0.35?'ميل هبوطي قوي':raw<-0.12?'ميل هبوطي':'محايد / انتظار';return{score:raw,prob,prob0,label,raw0,conf,calibrated};}
function roundLevels(p){if(!(p>0))return[];const e=Math.floor(Math.log10(p)),step=Math.pow(10,e-1),lo=Math.floor(p/step)*step;return[lo,lo+step].filter(x=>x>0&&x!==p);}
function fLevels(c){
  const cs=c.candles,p=c.p;if(!p||cs.length<60)return[];
  const L=[];const add=(pr,name,st)=>{if(pr&&isFinite(pr)&&pr>0)L.push({p:pr,name,strength:st});};
  const d=c.dayInfo;
  if(d){add(d.pdh,'قمة أمس (PDH)',3);add(d.pdl,'قاع أمس (PDL)',3);add(d.pdc,'إغلاق أمس',2);add(d.open,'افتتاح اليوم',2);add(d.wo,'افتتاح الأسبوع',2);
    if(d.high&&Math.abs(d.high-p)/p>0.0005)add(d.high,'قمة اليوم',2);if(d.low&&Math.abs(d.low-p)/p>0.0005)add(d.low,'قاع اليوم',2);}
  if(c.vwap)add(c.vwap,'VWAP اليومي',2);
  const closes=cs.map(x=>x.close);add(ema(closes,20),'EMA20 ('+c.tf+')',1);add(ema(closes,50),'EMA50 ('+c.tf+')',1);if(closes.length>=200)add(ema(closes,200),'EMA200 ('+c.tf+')',2);
  fPivots(cs,5,120).slice(-14).forEach(v=>add(v.p,(v.type==='high'?'قمة':'قاع')+' swing'+(v.eq?' (متساوية — سيولة)':''),v.eq?3:2));
  if(c.walls){if(c.walls.bid&&c.walls.bid[1]>0)add(c.walls.bid[0],'جدار شراء OB',2);if(c.walls.ask&&c.walls.ask[1]>0)add(c.walls.ask[0],'جدار بيع OB',2);}
  roundLevels(p).forEach(r=>add(r,'رقم دائري',1));
  if(c.extra)c.extra.forEach(x=>{if(Math.abs(x.p-p)/p<0.1)add(x.p,x.name,x.strength);});
  L.sort((a,b)=>a.p-b.p);const out=[];
  for(const l of L){const last=out[out.length-1];
    if(last&&Math.abs(l.p-last.p)/p<0.001){if(l.strength>last.strength0){last.name0=l.name;last.strength0=l.strength;last.p=l.p;}last.names.push(l.name);last.strength=Math.min(3.5,Math.max(l.strength,last.strength)+0.5);}
    else out.push({p:l.p,name:l.name,name0:l.name,strength:l.strength,strength0:l.strength,names:[l.name]});}
  out.forEach(l=>{l.full=l.names.join(' + ');l.name=l.names.length>1?l.name0+' (+'+(l.names.length-1)+')':l.name0;});
  return out;
}
function fPlan(c){
  const {p,a,lv,reg,score,used,tf}=c;const R={why:[]};
  const near=lv.filter(l=>l.p<p).slice(-1)[0],far=lv.filter(l=>l.p>p)[0];
  if(near&&far)R.watch=`أقرب دعم ${near.name} ${fp(near.p)} — أقرب مقاومة ${far.name} ${fp(far.p)}`;
  let dir=score>0.12?'long':score<-0.12?'short':null,setup='';
  if(!dir){
    if(reg&&reg.type==='range'){
      const lo=c.sw40.low,hi=c.sw40.high,w=hi-lo;
      if(w<2*a){R.wait=true;R.verdict='انتظار — range ضيّق';R.why.push(`المدى ${fp(lo)} – ${fp(hi)} أضيق من 2×ATR — ما في مجال لصفقة بـ R:R منيح. انطر كسر مع delta واضح`);return R;}
      if(p-lo<=0.6*a){dir='long';setup='range — شراء من القاع';R.entry=p-lo<=0.2*a?p:lo+0.15*a;R.entryName=R.entry===p?'السعر الحالي (عالقاع)':'Limit قرب قاع المدى';R.mode=R.entry===p?'market':'limit';R.sl=lo-0.5*a;R.slName='تحت قاع المدى';R.tps=[{p:lo+w*0.5,name:'نص المدى'},{p:hi-0.2*a,name:'قمة المدى'},{p:hi+0.5*a,name:'كسر المدى'}];R.why.push(`السوق range (ER ${reg.er.toFixed(2)}) والسعر لازق بقاع المدى ${fp(lo)} — الشراء من الحدود بـ stop ضيّق أفضل صفقة بالـ range`);}
      else if(hi-p<=0.6*a){dir='short';setup='range — بيع من القمة';R.entry=hi-p<=0.2*a?p:hi-0.15*a;R.entryName=R.entry===p?'السعر الحالي (عالقمة)':'Limit قرب قمة المدى';R.mode=R.entry===p?'market':'limit';R.sl=hi+0.5*a;R.slName='فوق قمة المدى';R.tps=[{p:hi-w*0.5,name:'نص المدى'},{p:lo+0.2*a,name:'قاع المدى'},{p:lo-0.5*a,name:'كسر المدى'}];R.why.push(`السوق range (ER ${reg.er.toFixed(2)}) والسعر لازق بقمة المدى ${fp(hi)} — البيع من الحدود بـ stop ضيّق أفضل صفقة بالـ range`);}
      else{R.wait=true;R.verdict='انتظار — range وإنت بالنص';R.why.push(`الميل محايد والسوق range: اشتري قرب ${fp(lo)} أو بيع قرب ${fp(hi)} بس مع delta مؤكّد — بالنص ما في صفقة`);return R;}
    }else{R.wait=true;R.verdict='انتظار — الميل محايد';R.why.push('ما في طرف مسيطر (العوامل متعادلة) — الانتظار صفقة. راقب التدفّق 1h وشمعة الإغلاق الجاية');return R;}
  }
  const m=dir==='long'?1:-1;R.dir=dir;
  /* v11.3 — ثلاث قواعد صلبة قبل أي دخول: */
  const WAIT=(verdict,why,extra)=>Object.assign({wait:true,verdict,why:[why],watch:R.watch||'',dirWanted:dir},extra||{});
  /* (2) زخم صادم عكس الاتجاه → انتظار (ما منعاكس حركة عمودية) */
  const th=c.thrust;if(th&&((th.dir==='up'&&dir==='short')||(th.dir==='down'&&dir==='long')))return WAIT('انتظار — زخم صادم عكس الاتجاه',`السعر تحرّك ${th.mult.toFixed(1)}×ATR بآخر ${th.k} شموع ${th.dir==='up'?'للفوق':'للتحت'} (${th.agree}/${th.k} شموع بنفس الاتجاه${th.delta?(th.delta>0)===(th.dir==='up')?' مع delta موافق':' بس الـ delta مش موافق':''}) — الميل ${dir==='short'?'هبوطي':'صعودي'} بس ما منعاكس زخم عمودي: انطر إغلاق شمعة عكسية أو pullback مع الاتجاه`,{thrustVeto:true});
  /* (5) الإطار الأكبر (1h/4h) كلّو عكس الصفقة → انتظار */
  if(c.htf){const hv=[c.htf.s1,c.htf.s4].filter(v=>v!=null);if(hv.length&&hv.every(v=>dir==='short'?v>0.12:v<-0.12))return WAIT('انتظار — عكس الإطار الأكبر',`${c.htf.s1!=null?'1h':''}${c.htf.s1!=null&&c.htf.s4!=null?' و':''}${c.htf.s4!=null?'4h':''} ${dir==='short'?'صاعدين':'هابطين'} والميل على ${tf} ${dir==='short'?'هبوطي':'صعودي'} — صفقة عكس التيار الأكبر؛ انطر توافق أو ادخل مع الإطار الأكبر على pullback`,{htfVeto:true});}
  /* (1) السيولة: ما في بيع تحت قمم متساوية ولا شراء فوق قيعان متساوية — الستوبات هونيك والسوق بيكسرها قبل ما يعكس */
  const liqAhead=lv.filter(l=>/متساوية/.test(l.full||l.name)&&(l.p-p)*m<0&&Math.abs(l.p-p)<=1.5*a).sort((x,y)=>Math.abs(x.p-p)-Math.abs(y.p-p))[0];
  if(liqAhead)return WAIT(`انتظار — ${dir==='short'?'قمم':'قيعان'} متساوية (سيولة) ${fp(liqAhead.p)} ${dir==='short'?'فوق':'تحت'} السعر`,`${dir==='short'?'البيع تحت قمم متساوية':'الشراء فوق قيعان متساوية'} = الوقوف قدّام الستوبات المتجمّعة؛ السوق عادةً بيكسرها (sweep) قبل ما يعكس — ${dir==='short'?'سيناريو 🎣 الكسر الكاذب للقمة (بيع بعد الإغلاق تحتها) أو كسر حقيقي للفوق':'سيناريو 🎣 الكسر الكاذب للقاع (شراء بعد الإغلاق فوقه) أو كسر حقيقي للتحت'} هو الصح`,{liqVeto:true,liqLevel:liqAhead.p});
  if(!setup){
    const e20=c.e20,ext=(p-e20)*m;
    let entry=p,mode='market',entryName='السعر الحالي';
    const pulls=lv.filter(l=>(l.p-p)*m<0&&Math.abs(l.p-p)>=0.3*a&&Math.abs(l.p-p)<=1.5*a).sort((x,y)=>Math.abs(x.p-p)-Math.abs(y.p-p));
    const pull=pulls.find(l=>l.strength>=2)||pulls[0];
    if(ext>0.8*a||used>1.2){
      if(pull){entry=pull.p;entryName=pull.name;}
      else{entry=dir==='long'?Math.max(e20,p-a):Math.min(e20,p+a);entryName=Math.abs(entry-e20)<1e-9?'EMA20 ('+tf+')':'pullback ~1×ATR';}
      mode='limit';setup=dir==='long'?'على pullback':'على bounce';
      R.why.push(ext>0.8*a?`السعر ممدود ${(ext/a).toFixed(1)}×ATR عن EMA20 — ما نلحقه، دخول Limit على ${entryName} ${fp(entry)}${used>1.2?' (واليوم استهلك '+Math.round(used*100)+'% من مداه)':''}`:`اليوم استهلك ${Math.round(used*100)}% من مداه الطبيعي (ATR يومي) — ما نطارد، ننطر رجوع لـ ${entryName} ${fp(entry)}`);
    }
    else{setup=reg&&reg.type==='trend'?'مع الترند':'Market';R.why.push(`السعر قريب من EMA20 (${(ext/a).toFixed(1)}×ATR)${used?' والحركة اليومية مستهلكة '+Math.round(used*100)+'% بس':''} — الدخول الآن مقبول`);}
    R.entry=entry;R.mode=mode;R.entryName=entryName;
    const stops=lv.filter(l=>(l.p-entry)*m<0&&l.strength>=2&&Math.abs(l.p-entry)>=0.6*a&&Math.abs(l.p-entry)<=3*a).sort((x,y)=>Math.abs(x.p-entry)-Math.abs(y.p-entry));
    if(stops.length){R.sl=stops[0].p-m*0.3*a;R.slName=(dir==='long'?'تحت ':'فوق ')+stops[0].name+' + 0.3×ATR';}
    else{const sw=c.sw20,s1=dir==='long'?sw.low-a*0.3:sw.high+a*0.3;if(Math.abs(s1-entry)<=3*a&&Math.abs(s1-entry)>=0.5*a){R.sl=s1;R.slName=(dir==='long'?'تحت آخر قاع':'فوق آخر قمة')+' (20 شمعة) + 0.3×ATR';}else{R.sl=entry-m*1.5*a;R.slName='1.5×ATR (ما في مستوى قريب)';}}
    const r=Math.abs(entry-R.sl);
    const tg=lv.filter(l=>(l.p-entry)*m>0).sort((x,y)=>Math.abs(x.p-entry)-Math.abs(y.p-entry));const ex=[];
    const pick=minR=>tg.find(l=>!ex.includes(l)&&(l.p-entry)*m/r>=minR&&(l.p-entry)*m/r<=minR+2.5);
    const t1=pick(1);const tps=[t1?{p:t1.p,name:t1.name}:{p:entry+m*r,name:'1R'}];if(t1)ex.push(t1);
    const r1=(tps[0].p-entry)*m/r,t2=pick(Math.max(2,r1+0.8));tps.push(t2?{p:t2.p,name:t2.name}:{p:entry+m*r*Math.max(2,r1+1),name:Math.max(2,r1+1).toFixed(1)+'R'});if(t2)ex.push(t2);
    const r2=(tps[1].p-entry)*m/r,t3=pick(Math.max(3,r2+0.8));tps.push(t3?{p:t3.p,name:t3.name}:{p:entry+m*r*Math.max(3,r2+1),name:Math.max(3,r2+1).toFixed(1)+'R'});
    R.tps=tps;
  }
  R.setup=setup;const r=Math.abs(R.entry-R.sl);R.r=r;R.tps.forEach(t=>t.r=(t.p-R.entry)*m/r);
  const pv=planValid(R);if(!pv.ok)return{wait:true,invalid:true,verdict:'انتظار — أرقام غير متناسقة',why:['البرنامج رفض الخطة: '+pv.why+' — غالباً لحظة بيانات غير متناسقة (سعر live مقابل شموع قديمة). بتتصلّح لحالها مع الشمعة الجاية'],watch:R.watch||''};
  return R;
}
function planValid(R){if(!R||R.wait)return{ok:true};const m=R.dir==='long'?1:-1,e=+R.entry,s=+R.sl,t1=R.tps&&R.tps[0]?+R.tps[0].p:NaN;
  if(!(isFinite(e)&&isFinite(s)&&e>0&&s>0))return{ok:false,why:'الدخول أو الـ stop مش رقم'};
  if((s-e)*m>=0)return{ok:false,why:'الـ stop بنفس جهة الهدف'};
  if(!(t1>0)||(t1-e)*m<=0)return{ok:false,why:'الهدف الأول بعكس الاتجاه'};
  if(Math.abs(e-s)/e<0.0003)return{ok:false,why:'مسافة الـ stop صفر تقريباً'};
  for(let i=1;i<R.tps.length;i++){if(!(R.tps[i].p>0)||(R.tps[i].p-R.tps[i-1].p)*m<0)return{ok:false,why:'الأهداف مش مرتّبة'};}
  return{ok:true};}
function fQuality(c){
  const list=[];const add=(ok,txt,w=1)=>list.push({ok,txt,w});const m=c.dir==='long'?1:-1;
  add(c.score*m>0.12,'التقييم الحالي بنفس اتجاه صفقتك ('+(c.label||'')+')',2);
  const h1=c.mtf&&c.mtf['1h'],h4=c.mtf&&c.mtf['4h'];add(!!(h1&&h4&&h1.s*m>0&&h4.s*m>0),'إطار 1h و4h موافقين',1.5);
  add(!!(c.reg&&c.reg.type==='trend'),'السوق بترند مش range',1);
  if(c.funding!=null){const f=c.funding*100;add(!(c.dir==='long'&&f>0.05)&&!(c.dir==='short'&&f<-0.02),'Funding مش عكسك بشكل متطرّف',1);}
  add(!c.evHigh,'ما في حدث مؤثر خلال ساعتين',1);
  add(!c.hot,'ما في خبر مؤثر بآخر 45 دقيقة',0.5);
  add(c.r2!=null&&c.r2>=2,'R:R للهدف التاني ≥ 1:2',1.5);
  add(c.r1!=null&&c.r1>=1,'الهدف الأول على الأقل 1R',0.5);
  if(c.walls&&c.walls.bid&&c.walls.ask){const wall=c.dir==='long'?c.walls.ask:c.walls.bid;const between=c.dir==='long'?(wall[0]>c.e&&wall[0]<c.t1):(wall[0]<c.e&&wall[0]>c.t1);add(!between,'ما في جدار كبير بين الدخول والهدف الأول',0.5);}
  if(c.flow1)add(c.flow1.net*m>0,'تدفّق السيولة آخر ساعة بنفس اتجاهك',1);
  add(!(c.used>1.2),'الحركة اليومية ما استُهلكت بعد (< 120% من ATR اليومي)',0.5);
  add(c.val<=c.acc*3,'رافعة ≤ x3',1);
  return list;
}
function qualityScore(list){const Wt=list.reduce((a,l)=>a+l.w,0)||1,S=list.reduce((a,l)=>a+(l.ok?l.w:0),0);return Math.round(S/Wt*10);}
function evPhase(h){return h>=0&&h<EV_PRE_H?'pre':h<0&&h>=-EV_POST_H?'post':'none';}
function fCondHit(kind,L,c,prev,avgV,a,st){
  st=st||{};const vol=c.vol,delta=c.delta,hi=c.high,lo=c.low,cl=c.close;a=a||Math.abs(L)*0.005;
  if(kind==='breakUp')return{hit:cl>L&&vol>=1.5*avgV&&delta>0,st};
  if(kind==='breakDown')return{hit:cl<L&&vol>=1.5*avgV&&delta<0,st};
  if(kind==='rejectUp')return{hit:hi>=L&&cl<L&&delta<0&&(hi-cl)>=0.3*a,st};
  if(kind==='bounceDown')return{hit:lo<=L&&cl>L&&delta>0&&(cl-lo)>=0.3*a,st};
  if(kind==='sweepLow'){ /* كسر كاذب للقيعان المتساوية (sell stops) → LONG بعد الرجوع فوقها خلال 3 شموع؛ 3 إغلاقات تحت المستوى = كسر حقيقي مش كاذب */
    st.below=cl<L?(st.below||0)+1:0;
    if(!st.swept){if(lo<L-0.05*a&&(!prev||prev.close>=L-0.05*a)){st.swept=true;st.wick=lo;st.n=0;}}else if(lo<st.wick)st.wick=lo;
    if(st.swept){st.n=(st.n||0)+1;if(cl>L&&delta>0){const sl=st.wick-0.3*a;st.swept=false;st.wick=null;st.n=0;return{hit:true,st,sl};}if(st.below>=3||st.n>4){st.swept=false;st.wick=null;st.n=0;}}
    return{hit:false,st};}
  if(kind==='sweepHigh'){st.above=cl>L?(st.above||0)+1:0;
    if(!st.swept){if(hi>L+0.05*a&&(!prev||prev.close<=L+0.05*a)){st.swept=true;st.wick=hi;st.n=0;}}else if(hi>st.wick)st.wick=hi;
    if(st.swept){st.n=(st.n||0)+1;if(cl<L&&delta<0){const sl=st.wick+0.3*a;st.swept=false;st.wick=null;st.n=0;return{hit:true,st,sl};}if(st.above>=3||st.n>4){st.swept=false;st.wick=null;st.n=0;}}
    return{hit:false,st};}
  if(kind==='retestUp'){ /* كسر مقاومة بحجم ثم إعادة اختبار → LONG على الإعادة */
    if(!st.broke){if(cl>L&&vol>=1.5*avgV&&delta>0){st.broke=true;st.n=0;}return{hit:false,st};}
    st.n=(st.n||0)+1;if(cl<L-0.3*a){st.broke=false;st.n=0;return{hit:false,st};}
    if(st.n>=1&&lo<=L+0.25*a&&cl>L&&delta>0)return{hit:true,st};
    if(st.n>12){st.broke=false;st.n=0;}return{hit:false,st};}
  if(kind==='retestDown'){if(!st.broke){if(cl<L&&vol>=1.5*avgV&&delta<0){st.broke=true;st.n=0;}return{hit:false,st};}
    st.n=(st.n||0)+1;if(cl>L+0.3*a){st.broke=false;st.n=0;return{hit:false,st};}
    if(st.n>=1&&hi>=L-0.25*a&&cl<L&&delta<0)return{hit:true,st};
    if(st.n>12){st.broke=false;st.n=0;}return{hit:false,st};}
  return{hit:false,st};
}
return{v:'11.3',W_DEFAULT,W_NAME,parseK,ema,fRegime,fAtr,fSwing,fPivots,fStructure,fShock,fThrust,fAgg,fMini,fTrend,fOIState,fFlowFactor,fFactors,fConf,fBias,fLevels,fPlan,planValid,fQuality,qualityScore,fCondHit,SCEN_KIND,sign,clamp,fmt};
})();
