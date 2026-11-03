/* FAM Flow — service worker v11.1
   حطّه جنب index.html بنفس المجلّد باسم sw.js (الصفحة بتسجّله لحالها إذا لقيته).
   - الصفحة نفسها: network-first (التحديثات بتوصل فوراً، وإذا ما في إنترنت بتفتح آخر نسخة)
   - المكتبات والخطوط (jsdelivr / cdnjs / Google Fonts / Firebase SDK): cache-first
   - كل الباقي (Binance، الـ worker، الأخبار…): بلا تدخّل — البيانات الحيّة ما بتنحفظ أبداً
   - أسماء الـ cache كلها بتبلّش بـ famflow- (ما بتلمس cache غيرها)، والردود opaque مقبولة */
const V='famflow-v10.8';const SHELL=V+'-shell',LIB=V+'-lib';
const LIBS=['https://cdn.jsdelivr.net/npm/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js'];
const isLib=u=>/(^|\.)(cdn\.jsdelivr\.net|cdnjs\.cloudflare\.com|fonts\.googleapis\.com|fonts\.gstatic\.com)$/.test(u.hostname)||(u.hostname==='www.gstatic.com'&&u.pathname.startsWith('/firebasejs/'));
const isShell=u=>u.origin===self.location.origin&&(u.pathname.endsWith('.html')||u.pathname.endsWith('/'));
self.addEventListener('install',e=>{e.waitUntil(caches.open(LIB).then(c=>Promise.allSettled(LIBS.map(x=>c.add(new Request(x,{mode:'no-cors'}))))).then(()=>self.skipWaiting()));});
self.addEventListener('activate',e=>{e.waitUntil(caches.keys().then(ks=>Promise.all(ks.filter(k=>k.startsWith('famflow-')&&k!==SHELL&&k!==LIB).map(k=>caches.delete(k)))).then(()=>self.clients.claim()));});
self.addEventListener('fetch',e=>{const r=e.request;if(r.method!=='GET')return;let u;try{u=new URL(r.url);}catch(x){return;}
  if(isShell(u)){e.respondWith(fetch(r).then(res=>{if(res&&res.ok){const cp=res.clone();caches.open(SHELL).then(c=>c.put(u.origin+u.pathname,cp));}return res;}).catch(()=>caches.match(u.origin+u.pathname).then(m=>m||Response.error())));return;}
  if(isLib(u)){e.respondWith(caches.match(r).then(m=>m||fetch(r).then(res=>{if(res&&(res.ok||res.type==='opaque')){const cp=res.clone();caches.open(LIB).then(c=>c.put(r,cp));}return res;})));return;}
});
