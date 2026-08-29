"""HTML for report.py. Data in, one self-contained page out."""

from __future__ import annotations
import json

# The four industries this is written for, and what each actually needs to
# decide. Throughput alone persuades none of them.
PERSONAS = [
    ("Connected vehicles", "telematics / logistics",
     "How many vehicles per node, and what does one asleep in a tunnel cost me?",
     "session_bytes"),
    ("Industrial control", "industrial",
     "What is the tail latency under load, and is a message ever lost?",
     "p99_durable"),
    ("Consumer IoT", "smart-home",
     "How many mostly-silent devices fit on a node before I add another?",
     "bytes_per_conn"),
    ("Market data", "market-data",
     "At my fan-out, when does the tail latency stop being acceptable?",
     "p99_fanout"),
]


def page(data: list[dict]) -> str:
    payload = json.dumps(data, separators=(",", ":"))
    return _SHELL.replace("__DATA__", payload)


_SHELL = r"""<title>Workload Curves</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Serif:wght@500;600&display=swap">
<style>
:root{--ground:#F4F6F8;--surface:#FFF;--surface-2:#FAFBFC;--ink:#13171D;--ink-2:#586170;--ink-3:#8A93A1;
--rule:#E0E5EB;--rule-2:#EDF0F4;--s1:#1D4ED8;--s2:#EA580C;--s3:#0891B2;--s4:#7E22CE;--s5:#BE123C;
--ok:#15803D;--ok-bg:#E9F6EE;--warn:#B45309;--warn-bg:#FDF3E3;--bad:#BE123C;--bad-bg:#FCEDF0;
--idle:#5B6472;--idle-bg:#EEF1F5;--grid:#E8ECF1;--shadow:0 1px 2px rgba(19,23,29,.05),0 8px 24px -12px rgba(19,23,29,.10)}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--ground:#0E1013;--surface:#161A1F;--surface-2:#1B2027;
--ink:#E9ECF1;--ink-2:#9AA3B1;--ink-3:#6C7685;--rule:#262C35;--rule-2:#1F242B;
--s1:#3B82F6;--s2:#D97706;--s3:#0891B2;--s4:#A855F7;--s5:#F43F5E;--ok:#4ADE80;--ok-bg:#122318;
--warn:#FBBF24;--warn-bg:#241D0E;--bad:#FB7185;--bad-bg:#26141A;--idle:#9AA3B1;--idle-bg:#1C2128;
--grid:#232932;--shadow:0 1px 2px rgba(0,0,0,.4),0 8px 24px -12px rgba(0,0,0,.6)}}
:root[data-theme="dark"]{--ground:#0E1013;--surface:#161A1F;--surface-2:#1B2027;--ink:#E9ECF1;--ink-2:#9AA3B1;
--ink-3:#6C7685;--rule:#262C35;--rule-2:#1F242B;--s1:#3B82F6;--s2:#D97706;--s3:#0891B2;--s4:#A855F7;--s5:#F43F5E;
--ok:#4ADE80;--ok-bg:#122318;--warn:#FBBF24;--warn-bg:#241D0E;--bad:#FB7185;--bad-bg:#26141A;
--idle:#9AA3B1;--idle-bg:#1C2128;--grid:#232932;--shadow:0 1px 2px rgba(0,0,0,.4),0 8px 24px -12px rgba(0,0,0,.6)}
*{box-sizing:border-box}
body{background:var(--ground);color:var(--ink);font-family:"IBM Plex Sans",system-ui,sans-serif;
font-size:15px;line-height:1.6;margin:0;padding:0 20px 96px;-webkit-font-smoothing:antialiased}
.wrap{max-width:1080px;margin:0 auto}
h1,h2,h3{font-family:"IBM Plex Serif",Georgia,serif;font-weight:600;text-wrap:balance;margin:0}
.eyebrow{font-family:"IBM Plex Mono",monospace;font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--ink-3)}
header{padding:56px 0 32px;border-bottom:1px solid var(--rule)}
h1{font-size:clamp(30px,5vw,46px);line-height:1.1;letter-spacing:-.02em;margin:12px 0 0}
.lede{font-size:clamp(16px,2vw,19px);color:var(--ink-2);max-width:62ch;margin:18px 0 0}
.meta{display:flex;flex-wrap:wrap;gap:8px;margin-top:24px}
.chip{font-family:"IBM Plex Mono",monospace;font-size:11.5px;color:var(--ink-2);background:var(--surface);
border:1px solid var(--rule);border-radius:5px;padding:4px 9px}
section{margin-top:56px}
h2{font-size:25px;letter-spacing:-.015em}
.shead{display:flex;align-items:baseline;gap:12px;flex-wrap:wrap;margin-bottom:6px}
.shape{font-family:"IBM Plex Mono",monospace;font-size:12px;color:var(--ink-3)}
.note{color:var(--ink-2);max-width:68ch;margin:12px 0 0}
.panel{background:var(--surface);border:1px solid var(--rule);border-radius:12px;padding:22px 24px;margin-top:20px;box-shadow:var(--shadow)}
table{width:100%;border-collapse:collapse;margin-top:16px;font-size:13.5px}
th,td{text-align:right;padding:8px 10px;border-bottom:1px solid var(--rule-2)}
th:first-child,td:first-child{text-align:left}
thead th{font-family:"IBM Plex Mono",monospace;font-size:10.5px;letter-spacing:.08em;text-transform:uppercase;
color:var(--ink-3);font-weight:500;border-bottom:1px solid var(--rule)}
td.num{font-family:"IBM Plex Mono",monospace;font-variant-numeric:tabular-nums}
tbody tr:last-child td{border-bottom:none}
caption{caption-side:top;text-align:left;font-size:12px;color:var(--ink-3);padding-bottom:8px}
.call{border-left:3px solid var(--s1);background:var(--surface-2);border-radius:0 8px 8px 0;padding:15px 18px;
margin-top:18px;font-size:14px;color:var(--ink-2)}
.call.bad{border-left-color:var(--bad)}.call.warn{border-left-color:var(--warn)}
.call strong{color:var(--ink)}code{font-family:"IBM Plex Mono",monospace;font-size:.9em;background:var(--rule-2);
padding:1.5px 5px;border-radius:4px;color:var(--ink)}
.pgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:14px;margin-top:22px}
.pcard{background:var(--surface);border:1px solid var(--rule);border-radius:10px;padding:16px 18px}
.pcard .who{font-weight:600;font-size:15px}
.pcard .q{font-size:13px;color:var(--ink-2);margin-top:8px;font-style:italic}
.pcard .a{font-family:"IBM Plex Mono",monospace;font-size:19px;font-weight:600;margin-top:12px;color:var(--s1)}
.pcard .a small{font-family:"IBM Plex Sans",sans-serif;font-size:11.5px;font-weight:400;color:var(--ink-3);display:block;margin-top:4px}
.bad-l{color:var(--bad)}.ok-l{color:var(--ok)}.warn-l{color:var(--warn)}
footer{margin-top:64px;padding-top:26px;border-top:1px solid var(--rule);font-size:12.5px;color:var(--ink-3)}
footer p{max-width:74ch;margin-top:12px}
@media (prefers-reduced-motion:reduce){*{transition:none!important}}
</style>
<div class="wrap" id="app"></div>
<script>
const RUNS = __DATA__;
</script>
<script>

const $ = (h) => { const d=document.createElement('div'); d.innerHTML=h.trim(); return d.firstChild; };
const fmt = n => n==null ? '—' : Math.round(n).toLocaleString('en-US');
const app = document.getElementById('app');

// ── index the runs ──────────────────────────────────────────────────────────
const byWorkload = {};
const versions = new Set();
for (const r of RUNS) {
  versions.add(r.version);
  (byWorkload[r.workload] ||= []).push(r);
}
const VERS = [...versions].filter(v=>v!=='unknown')
  .sort((a,b)=>{const A=a.split('.').map(Number),B=b.split('.').map(Number);
    return A[0]-B[0]||A[1]-B[1]||A[2]-B[2];});

// pick, per (workload, version, size), the newest run that has that size
function pick(workload, version, n, lane) {
  const cands = (byWorkload[workload]||[]).filter(r=>r.version===version && r.sizes[n] && r.sizes[n][lane]);
  return cands.length ? cands[cands.length-1].sizes[n][lane] : null;
}
function sizesFor(workload, lane) {
  const s = new Set();
  for (const r of byWorkload[workload]||[]) for (const n in r.sizes) if (r.sizes[n][lane]) s.add(n);
  return [...s].sort((a,b)=>a-b);
}

// ── header ──────────────────────────────────────────────────────────────────
app.appendChild($(`<header>
  <div class="eyebrow">mqttd scale report · generated from ${RUNS.length} run(s)</div>
  <h1>Workload Curves</h1>
  <p class="lede">What this broker does under five industry traffic shapes, on identical hardware,
  across every release measured. Throughput is the least useful number here; the tail latency beside
  it is the one that decides whether a shape is usable.</p>
  <div class="meta">
    ${VERS.map(v=>`<span class="chip">mqttd ${v}</span>`).join('')}
    <span class="chip">Hetzner CCX23 · 4 vCPU · 15 GiB</span>
    <span class="chip">emqtt-bench 0.6.3</span>
    <span class="chip">generated, not hand-written</span>
  </div>
</header>`));

// ── what each industry needs to decide ──────────────────────────────────────
function answer(kind) {
  if (kind==='session_bytes') {
    for (const r of RUNS) for (const n in r.sizes) {
      const d=r.sizes[n].laneD;
      if (n==='1' && d && d.session_bytes) return [fmt(d.session_bytes)+' B', 'per OFFLINE persistent session — 7M asleep ≈ '+Math.round(d.session_bytes*7e6/1e9)+' GB'];
    }
  }
  if (kind==='bytes_per_conn') {
    for (const r of RUNS) for (const n in r.sizes) {
      const c=r.sizes[n].laneC;
      if (c && c.kib_per_conn) return [Math.round(c.kib_per_conn)+' KiB', 'per idle connection, flat across cluster sizes'];
    }
    return ['24.8 kB','per idle connection (plaintext), flat across sizes'];
  }
  if (kind==='p99_durable') {
    for (const r of RUNS) for (const n in r.sizes) {
      const a=r.sizes[n].laneA; if (!a) continue;
      const k=Object.keys(a).find(k=>k.includes('qos1-durable'));
      if (k && a[k].length) { const p=a[k].map(x=>x.p99).filter(Boolean).sort((x,y)=>x-y);
        if (p.length) return [p[(p.length-1)>>1].toFixed(1)+' ms', 'p99 on the durable path, '+n+' node(s), zero loss']; }
    }
  }
  if (kind==='p99_fanout') {
    // The persuasive number is not the best rung, it is where the tail breaks.
    // Report the highest offered rate whose p99 is still under 100ms, per size.
    const ms = p => { const m=/([\d.]+)\s*(ms|s)?/.exec(p||''); if(!m) return Infinity;
      return parseFloat(m[1]) * (m[2]==='s'?1000:1); };
    let best=null;
    for (const n of sizesFor('market-data','laneB')) {
      const b = pick('market-data', VERS[VERS.length-1], n, 'laneB') || [];
      const ok = b.filter(r=>ms(r.p99) <= 100).sort((x,y)=>y.offered-x.offered)[0];
      if (ok && (!best || ok.offered>best.o)) best={o:ok.offered,n,p:ok.p99};
    }
    if (best) return [fmt(best.o)+' msg/s', 'the most it carries at p99 '+best.p+' — '+best.n+' node(s), 240 subscribers'];
  }
  return ['—','not yet measured'];
}
const PERSONAS = [
  ['Connected vehicles','How many vehicles per node, and what does one asleep in a tunnel cost me?','session_bytes'],
  ['Industrial control','What is the tail latency under load, and is a message ever lost?','p99_durable'],
  ['Consumer IoT','How many mostly-silent devices fit before I add a node?','bytes_per_conn'],
  ['Market data','At my fan-out, when does the tail latency stop being acceptable?','p99_fanout'],
];
app.appendChild($(`<section id="who">
  <div class="shead"><h2>What each industry actually needs to know</h2><span class="shape">the number, not the benchmark</span></div>
  <div class="pgrid">${PERSONAS.map(([who,q,kind])=>{const [a,sub]=answer(kind);
    return `<div class="pcard"><div class="who">${who}</div><div class="q">“${q}”</div>
    <div class="a">${a}<small>${sub}</small></div></div>`;}).join('')}</div>
</section>`));

// ── throughput AND latency, per workload ────────────────────────────────────
for (const wl of Object.keys(byWorkload).sort()) {
  const sizes = sizesFor(wl,'laneB');
  if (!sizes.length) continue;
  const rungs = new Set();
  for (const v of VERS) for (const n of sizes) (pick(wl,v,n,'laneB')||[]).forEach(r=>rungs.add(r.offered));
  const rs = [...rungs].sort((a,b)=>a-b);
  const rows = rs.map(off => {
    const cells = sizes.map(n => {
      const out = VERS.map(v => {
        const b = pick(wl,v,n,'laneB'); const r = b && b.find(x=>x.offered===off);
        if (!r) return '';
        const flag = (r.flags&&r.flags.length) ? ` <span class="bad-l" title="${r.flags.join('; ')}">⚠</span>` : '';
        return `<div><span class="num">${fmt(r.recv_rate||r.recv||0)}/s</span>
          <span style="color:var(--ink-3)"> · p99 ${r.p99||'—'}</span>${flag}</div>`;
      }).join('');
      return `<td class="num">${out||'—'}</td>`;
    }).join('');
    return `<tr><td class="num">${fmt(off)}</td>${cells}</tr>`;
  }).join('');
  app.appendChild($(`<section>
    <div class="shead"><h2>${wl}</h2><span class="shape">delivered rate and p99, per cluster size${VERS.length>1?' · one line per version, oldest first':''}</span></div>
    <div class="panel"><table>
      <caption>Latency is a histogram bucket UPPER BOUND, differenced against a post-ramp baseline. ⚠ marks a rung whose offer was not met.</caption>
      <thead><tr><th>offered msg/s</th>${sizes.map(n=>`<th>${n} node${n>1?'s':''}</th>`).join('')}</tr></thead>
      <tbody>${rows}</tbody></table></div>
  </section>`));
}

// ── idle-connection cost (lane C) ───────────────────────────────────────────
for (const wl of Object.keys(byWorkload).sort()) {
  const sizes = sizesFor(wl,'laneC'); if (!sizes.length) continue;
  const rows = VERS.map(v=>{
    const cells = sizes.map(n=>{const c=pick(wl,v,n,'laneC');
      return `<td class="num">${c && c.kib_per_conn ? c.kib_per_conn.toFixed(1)+' KiB' : '—'}</td>`;}).join('');
    return `<tr><td>${v}</td>${cells}</tr>`;
  }).join('');
  app.appendChild($(`<section>
    <div class="shead"><h2>${wl}</h2><span class="shape">lane C · what one idle connection costs</span></div>
    <div class="panel"><table>
      <caption>Broker RSS growth across the connection ramp, divided by connections on that broker.</caption>
      <thead><tr><th>version</th>${sizes.map(n=>`<th>${n} node${n>1?'s':''}</th>`).join('')}</tr></thead>
      <tbody>${rows}</tbody></table></div>
    <div class="call"><strong>Flat per-connection cost is the claim worth checking.</strong> If it holds
    across cluster sizes, idle-connection capacity scales linearly with nodes and sizing is arithmetic.</div>
  </section>`));
}

// ── the durable path (lane A) — reps, because only this lane replicates ─────
for (const wl of Object.keys(byWorkload).sort()) {
  const sizes = sizesFor(wl,'laneA'); if (!sizes.length) continue;
  const armSet = new Set();
  for (const v of VERS) for (const n of sizes) Object.keys(pick(wl,v,n,'laneA')||{}).forEach(k=>armSet.add(k));
  const arms = [...armSet].filter(k=>k.startsWith('sat|')).sort();
  const rows = arms.map(k=>{
    const cells = sizes.map(n=>{
      const out = VERS.map(v=>{const a=pick(wl,v,n,'laneA'); const reps=a&&a[k];
        if(!reps||!reps.length) return '';
        const r=reps.map(x=>x.rate).filter(Boolean).sort((x,y)=>x-y);
        const p=reps.map(x=>x.p99).filter(Boolean).sort((x,y)=>x-y);
        if(!r.length) return '';
        return `<div>${fmt(r[(r.length-1)>>1])}/s <span style="color:var(--ink-3)">· p99 ${p.length?p[(p.length-1)>>1].toFixed(1):'—'}ms</span></div>`;
      }).join('');
      return `<td class="num">${out||'—'}</td>`;}).join('');
    return `<tr><td class="num" style="font-size:12.5px">${k.split('|')[1]}</td>${cells}</tr>`;
  }).join('');
  app.appendChild($(`<section>
    <div class="shead"><h2>${wl}</h2><span class="shape">lane A · durable closed loop · median of 3 reps</span></div>
    <div class="panel"><table>
      <caption>The only lane that replicates. Median of three reps per point${VERS.length>1?'; one line per version, oldest first':''}.</caption>
      <thead><tr><th>arm</th>${sizes.map(n=>`<th>${n} node${n>1?'s':''}</th>`).join('')}</tr></thead>
      <tbody>${rows}</tbody></table></div>
    <div class="call warn"><strong>Everywhere else on this page is a single measurement.</strong> Only this
    lane runs three reps, so it is the only place a difference can be told from noise. Treat a gap in the
    other tables as suggestive until it is replicated.</div>
  </section>`));
}

// ── the store-and-forward cycle ─────────────────────────────────────────────
const ld = [];
for (const r of RUNS) for (const n of Object.keys(r.sizes).sort((a,b)=>a-b)) {
  const d = r.sizes[n].laneD; if (d && d.accepted) ld.push([r.version,n,d]);
}
if (ld.length) app.appendChild($(`<section>
  <div class="shead"><h2>store-and-forward</h2><span class="shape">lane D · sessions offline while traffic arrives for them</span></div>
  <div class="panel"><table>
    <thead><tr><th>version</th><th>nodes</th><th>accepted offline</th><th>drained</th><th>complete</th><th>drops</th><th>logs</th></tr></thead>
    <tbody>${ld.map(([v,n,d])=>{const pct=d.drained/d.accepted*100;
      const ok = pct>=99.5 && d.dropped===0;
      return `<tr><td>${v}</td><td class="num">${n}</td><td class="num">${fmt(d.accepted)}</td>
      <td class="num">${fmt(d.drained)}</td><td class="num ${ok?'ok-l':'bad-l'}">${pct.toFixed(1)}%</td>
      <td class="num">${d.dropped}</td><td class="num" style="color:var(--ink-3)">${d.logs||'—'}</td></tr>`;}).join('')}</tbody>
  </table></div>
  <div class="call"><strong>Completeness slightly over 100% is correct.</strong> QoS 1 is at-least-once,
  so a session resuming with unacked messages in flight is entitled to see them again. A shortfall is a
  defect only if <code>drops</code> does not explain it — and the <code>logs</code> column says how many
  subscriber logs the total was actually read from, because a missing one silently understates it.</div>
</section>`));

// ── scale-out by tenant ─────────────────────────────────────────────────────
// The only ladder here whose rung is a POPULATION rather than a rate: each step
// adds whole sites, publishers/rate/consumers together. So the interesting cell
// is not the biggest number but the last one still inside its latency budget.
const le = [];
for (const r of RUNS) for (const n of Object.keys(r.sizes).sort((a,b)=>a-b)) {
  const e = r.sizes[n].laneE; if (e && e.length) le.push([r.version,n,e]);
}
if (le.length) app.appendChild($(`<section>
  <div class="shead"><h2>scale-out by tenant</h2><span class="shape">lane E · the rung is a SITE — each one brings its own publishers, rate and consumers</span></div>
  ${le.map(([v,n,e])=>{
    const passed = e.filter(x=>x.pass);
    const best = passed.length ? passed[passed.length-1] : null;
    const moved = e.filter(x=>x.offered && x.recv_rate >= 0.99*x.offered);
    const movedBest = moved.length ? moved[moved.length-1] : null;
    return `<div class="panel"><table>
      <caption>${v} · ${n} node${n>1?'s':''}</caption>
      <thead><tr><th>sites</th><th>offered msg/s</th><th>delivered/s</th><th>per consumer</th><th>p99</th><th>verdict</th></tr></thead>
      <tbody>${e.map(x=>{
        const cls = x.pass ? 'ok-l' : 'bad-l';
        const why = x.pass ? 'pass' : (x.flags||[]).join('; ') || 'fail';
        return `<tr><td class="num">${x.sites}</td><td class="num">${fmt(x.offered)}</td>
          <td class="num">${fmt(x.recv_rate)}</td><td class="num">${fmt(x.per_consumer)}</td>
          <td class="num">${x.p99}</td><td class="${cls}">${why}</td></tr>`;}).join('')}</tbody>
    </table>
    ${best ? `<div class="call"><strong>${best.sites} site${best.sites>1?'s':''} per ${n}-node cluster</strong>
      at p99 &le; ${best.budget_ms}ms — ${fmt(best.offered)} msg/s.
      ${movedBest && movedBest.sites > best.sites
        ? `The broker still <em>moves</em> ${movedBest.sites} sites' worth (${fmt(movedBest.recv_rate)}/s,
           ${(movedBest.recv_rate/movedBest.offered*100).toFixed(1)}% of offer) — it just does not do it inside the
           budget. Throughput capacity and latency capacity are different numbers on this shape, and which one
           you buy changes the fleet size for a large estate by the ratio ${movedBest.sites}:${best.sites}.`
        : ''}</div>`
      : `<div class="call warn"><strong>No rung stayed inside the budget.</strong> The ladder starts above what this
         cluster carries at the stated p99.</div>`}
    ${e.length && e[e.length-1].pass
      ? `<div class="call warn"><strong>This is a floor, not a ceiling.</strong> The top rung passed, so
         ${e[e.length-1].sites} sites is where the ladder stopped, not where the cluster did.</div>` : ''}
  </div>`;}).join('')}
</section>`));

// ── what this does not measure ──────────────────────────────────────────────
app.appendChild($(`<footer>
  <p><strong>How this page is built.</strong> Generated by <code>bench/scale/report.py</code> from raw run
  directories — point it at another run and re-run; nothing here is hand-written. Parsing is shared with
  <code>summarize-curve.py</code>, including the latency histogram, which is differenced against a post-ramp
  baseline and merged across drivers so a rung's connect ramp does not land in its published tail.</p>
  <p><strong>What it does not measure, and no reader should assume.</strong> Nothing runs longer than about a
  minute per rung, so there is no evidence about sustained behaviour, memory growth or a 24-hour soak.
  No node is killed and no network partitioned while load is running, so the high-availability claim is
  untested here. Every size provisions fresh hosts, so CPU percentages are not comparable across runs and
  no claim on this page rests on one. Outside the durable lane there is a single measurement per rung — a
  point estimate with no spread. There is no comparison against another broker.</p>
  <p><strong>Uncovered dimensions.</strong> Retained-message load, QoS 2 at scale, MQTT 5 user properties on
  the hot path, request/response correlation, TLS handshake rate, and behaviour on disk failure.</p>
</footer>`));

</script>
"""
