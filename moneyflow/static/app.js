/* Money Flow Map dashboard: vanilla JS, no external requests.
   Data: WebSocket /ws (all timeframes, ~1 update/s), falling back to polling /api/snapshot.
   Rendering: the network is drawn into an offscreen canvas only when data changes; the visible
   canvas copies it and adds the moving particles at <= 30 fps. Animation stops while the tab is
   hidden, when "motion" is off, or when the OS asks for reduced motion. */
(() => {
  "use strict";
  const $ = (id) => document.getElementById(id);
  const store = {
    get(k, d) { try { const v = localStorage.getItem("mf." + k); return v === null ? d : JSON.parse(v); } catch { return d; } },
    set(k, v) { try { localStorage.setItem("mf." + k, JSON.stringify(v)); } catch { /* private mode */ } },
  };
  const reduceMotion = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  const S = {
    data: null, tf: store.get("tf", null), motion: store.get("motion", !reduceMotion), hidden: document.hidden,
    serverOffset: 0, hover: null, layout: null, staticCanvas: document.createElement("canvas"),
    particles: [], lastFrame: 0, newsVersion: -1, lastPanel: 0, receivedAt: 0,
  };
  const COL = { in: "#22d38a", out: "#ff4d6d", neu: "#8b97a8", amber: "#f2b84b", text: "#e8edf3", muted: "#8592a3" };
  const canvas = $("map"), ctx = canvas.getContext("2d");
  // self-measurement (read with window.__mfPerf in the console): time spent in our own code
  const perf = window.__mfPerf = { since: performance.now(), frames: 0, frameMs: 0, statics: 0, staticMs: 0, panels: 0, panelMs: 0, msgs: 0, parseMs: 0,
    reset() { Object.assign(this, { since: performance.now(), frames: 0, frameMs: 0, statics: 0, staticMs: 0, panels: 0, panelMs: 0, msgs: 0, parseMs: 0 }); },
    report() { const s = (performance.now() - this.since) / 1000, busy = this.frameMs + this.staticMs + this.panelMs + this.parseMs;
      return { seconds: +s.toFixed(1), fps: +(this.frames / s).toFixed(1), ms_per_frame: +(this.frameMs / Math.max(1, this.frames)).toFixed(3),
        static_redraws_per_s: +(this.statics / s).toFixed(2), ms_per_static: +(this.staticMs / Math.max(1, this.statics)).toFixed(2),
        ms_per_panel: +(this.panelMs / Math.max(1, this.panels)).toFixed(2), ms_per_message_parse: +(this.parseMs / Math.max(1, this.msgs)).toFixed(2),
        script_ms_per_s: +(busy / s).toFixed(2), script_share_of_one_core_pct: +(busy / s / 10).toFixed(2) }; } };
  const timed = (key, fn) => function (...a) { const t = performance.now(); try { return fn.apply(this, a); } finally { perf[key + "Ms"] += performance.now() - t; perf[key === "frame" ? "frames" : key === "static" ? "statics" : key === "panel" ? "panels" : "msgs"]++; } };

  // ── formatting ──────────────────────────────────────────────────────────────────────
  const fmtPct = (v, d = 2) => v == null ? "—" : (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(d) + "%";
  const fmtZ = (v) => v == null ? "—" : (v >= 0 ? "+" : "−") + Math.abs(v).toFixed(1) + "σ";
  const cls = (v) => v == null ? "flat" : v > 0 ? "up" : v < 0 ? "down" : "flat";
  const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const fmtAge = (s) => s == null ? "—" : s < 1 ? (s * 1000).toFixed(0) + "ms" : s < 90 ? s.toFixed(s < 10 ? 1 : 0) + "s" :
    s < 5400 ? Math.round(s / 60) + "m" : s < 172800 ? (s / 3600).toFixed(1) + "h" : Math.round(s / 86400) + "d";
  const fmtUsd = (v) => { if (v == null) return "—"; const a = Math.abs(v), s = v < 0 ? "−" : "+";
    return a >= 1e9 ? s + "$" + (a / 1e9).toFixed(2) + "B" : a >= 1e6 ? s + "$" + (a / 1e6).toFixed(1) + "M" : s + "$" + (a / 1e3).toFixed(0) + "K"; };
  const fmtPx = (v) => v == null ? "—" : v >= 1000 ? v.toLocaleString(undefined, { maximumFractionDigits: 2 }) :
    v >= 10 ? v.toFixed(2) : v.toFixed(4);
  const nowServer = () => Date.now() / 1000 + S.serverOffset;
  const tfData = () => S.data && S.data.timeframes && S.data.timeframes[S.tf];
  const statusBadge = (st, extra = "") => `<span class="badge b-${esc(st)}">${esc((st || "").replace("_", " ").toUpperCase())}${extra}</span>`;

  // ── data connection ─────────────────────────────────────────────────────────────────
  let ws = null, wsDelay = 1000, pollTimer = null;
  function connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const tok = new URLSearchParams(location.search).get("token");
    try { ws = new WebSocket(`${proto}://${location.host}/ws?tf=all${tok ? "&token=" + encodeURIComponent(tok) : ""}`); }
    catch { return startPolling(); }
    ws.onopen = () => { wsDelay = 1000; setConn(true, "live stream"); stopPolling(); };
    ws.onmessage = (ev) => { try { const t = performance.now(); const m = JSON.parse(ev.data); perf.parseMs += performance.now() - t; perf.msgs++;
      if (m.type === "snapshot") onData(m.data); } catch (e) { console.error(e); } };
    ws.onclose = () => {
      if (!/token/.test($("footConn").textContent))
        setConn(false, wsDelay < 8000 ? "stream lost, retrying"
          : "not connected: the service may have stopped" + (S.idleS ? ` (it stops by itself ${Math.round(S.idleS / 60)} min after the last viewer leaves)` : "")
            + ". Start it again with start.bat.");
      startPolling(); setTimeout(connect, wsDelay); wsDelay = Math.min(30000, wsDelay * 2);
    };
    ws.onerror = () => { try { ws.close(); } catch { /* ignore */ } };
  }
  function startPolling() { if (pollTimer) return; pollTimer = setInterval(poll, 5000); poll(); }
  function stopPolling() { if (pollTimer) { clearInterval(pollTimer); pollTimer = null; } }
  async function poll() {
    try { const tok = new URLSearchParams(location.search).get("token");
      const r = await fetch("/api/snapshot?tf=all" + (tok ? "&token=" + encodeURIComponent(tok) : ""), { cache: "no-store" });
      if (r.status === 401) setConn(false, tok ? "wrong token in the address (?token=…)" : "this server needs a token: open it as …/?token=YOUR_TOKEN");
      if (r.ok) onData(await r.json()); } catch { /* server down */ }
  }
  function setConn(ok, text) { const el = $("footConn"); el.textContent = text; el.className = "conn " + (ok ? "ok" : "bad"); }

  function onData(d) {
    S.data = d;
    S.receivedAt = Date.now() / 1000;
    S.serverOffset = d.generated_ts - S.receivedAt;
    const idle = d.meta && d.meta.idle_shutdown_s;              // the service stops by itself when unused
    if (idle !== S.idleS) {
      S.idleS = idle;
      $("footIdle").textContent = idle ? `This program stops by itself ${Math.round(idle / 60)} min after the last viewer leaves.`
                                       : "This program keeps running until stopped (auto-stop off).";
    }
    if (S.hidden) { S.dirty = true; return; }              // hidden tab: keep the data, draw nothing
    render();
  }
  function render() {
    const d = S.data; if (!d) return;
    S.dirty = false;
    if (!S.tf || !d.timeframes[S.tf]) S.tf = d.default_timeframe || "today";
    markTf();
    drawStatic();
    buildParticles();
    if (Date.now() - S.lastPanel > 900 || !S.lastPanel) { renderPanel(); S.lastPanel = Date.now(); }
    if (d.news_version !== S.newsVersion) {
      // new headlines join at the end of the current scroll cycle, so the ticker never jumps back
      if (S.newsVersion < 0 || !$("wireInner").querySelector(".wi")) renderWire(); else S.wirePending = true;
      S.newsVersion = d.news_version;
    }
    if (!S.motion || S.hidden) blit();
    if (S.hover) showTip(S.hover);
  }

  // ── timeframe & motion controls ────────────────────────────────────────────────────
  document.querySelectorAll(".tfs button").forEach((b) => b.addEventListener("click", () => {
    S.tf = b.dataset.tf; store.set("tf", S.tf); markTf(); drawStatic(); buildParticles(); renderPanel(); blit();
    if (S.hover) showTip(S.hover);
  }));
  function markTf() { document.querySelectorAll(".tfs button").forEach((b) => b.classList.toggle("on", b.dataset.tf === S.tf)); }
  const motionBtn = $("motionBtn");
  function markMotion() { motionBtn.textContent = S.motion ? "MOTION ON" : "MOTION OFF"; document.body.classList.toggle("still", !S.motion); }
  motionBtn.addEventListener("click", () => { S.motion = !S.motion; store.set("motion", S.motion); markMotion(); if (S.motion) loop(); else blit(); });
  markMotion();
  document.addEventListener("visibilitychange", () => {
    S.hidden = document.hidden; document.body.classList.toggle("hidden", S.hidden);
    if (!S.hidden) { if (S.dirty) { S.lastPanel = 0; S.newsVersion = -1; render(); } if (S.motion) loop(); }
  });

  // ── geometry ────────────────────────────────────────────────────────────────────────
  function layout() {
    const r = canvas.getBoundingClientRect();
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    const W = Math.max(320, r.width), H = Math.max(300, r.height);
    for (const c of [canvas, S.staticCanvas]) { c.width = Math.round(W * dpr); c.height = Math.round(H * dpr); }
    const narrow = W < 560;                                   // phones: two columns, compact labels
    const R = narrow ? 17 : Math.max(17, Math.min(38, Math.min(W * 0.6, H) * 0.046));
    const legendH = narrow ? 0 : (document.querySelector(".legend") || {}).offsetHeight || 20;   // it wraps when narrow
    S.layout = narrow ? { W, H, dpr, R, narrow, padX: R * 2.6, padTop: R + 30, padBot: R + 70 }
                      : { W, H, dpr, R, narrow, padX: R * 3.2, padTop: R * 1.5, padBot: R + 44 + legendH };
    S.narrowPos = null;
  }
  /* On narrow screens the configured x/y would crowd three nodes into a row: nodes are re-flowed
     into a left and a right column (by their configured side, top to bottom); a node configured in
     the middle (x = 0.5) takes the top row on its own. The Fed stays in the centre. */
  function narrowPositions(nodes) {
    const mid = nodes.filter((n) => Math.abs((n.x ?? 0.5) - 0.5) < 0.05);
    const left = nodes.filter((n) => (n.x ?? 0.5) < 0.45).sort((a, b) => a.y - b.y);
    const right = nodes.filter((n) => (n.x ?? 0.5) > 0.55).sort((a, b) => a.y - b.y);
    const rows = Math.max(left.length, right.length) + (mid.length ? 1 : 0);
    const out = {}, step = rows > 1 ? 1 / (rows - 1) : 0;
    mid.forEach((n, i) => { out[n.id] = { x: 0.5 + (i - (mid.length - 1) / 2) * 0.3, y: 0 }; });
    const off = mid.length ? 1 : 0;
    left.forEach((n, i) => { out[n.id] = { x: 0.12, y: (i + off) * step }; });
    right.forEach((n, i) => { out[n.id] = { x: 0.88, y: (i + off) * step }; });
    return out;
  }
  function pos(n) {
    const L = S.layout;
    let x = n.x ?? 0.5, y = n.y ?? 0.5;
    if (L.narrow && n.id) {
      const td = tfData();
      // re-flow when the window's node set changes (a notable-stock slot comes and goes between windows)
      const key = td ? td.nodes.map((x) => x.id).join(",") : "";
      if (td && (!S.narrowPos || S.narrowKey !== key)) { S.narrowPos = narrowPositions(td.nodes); S.narrowKey = key; }
      const p = S.narrowPos && S.narrowPos[n.id];
      if (p) { x = p.x; y = p.y; }
    }
    return { x: L.padX + x * (L.W - 2 * L.padX), y: L.padTop + y * (L.H - L.padTop - L.padBot) };
  }
  function nodeColor(n) {
    if (n.z == null) return COL.neu;
    return n.direction === "inflow" ? COL.in : n.direction === "outflow" ? COL.out : COL.neu;
  }
  function curve(a, b, k) {
    const mx = (a.x + b.x) / 2, my = (a.y + b.y) / 2, dx = b.x - a.x, dy = b.y - a.y;
    const len = Math.hypot(dx, dy) || 1, nx = -dy / len, ny = dx / len;
    const c = S.layout, cx = c.W / 2, cy = c.H / 2;
    let side = ((mx - cx) * nx + (my - cy) * ny) > 0 ? 1 : -1;       // bow away from the centre
    let off = Math.min(90, len * 0.16);
    if (Math.abs(dx) < c.R * 2) {        // one above the other (a column): a near-straight line would hide
      side = -side;                      // under the spheres and labels between them, so bow inwards, clear
      off = Math.max(off, c.R * (c.narrow ? 4.2 : 2.4));   // (phones: past the labels under each node)
    }
    off *= side * k;
    return { x: mx + nx * off, y: my + ny * off };
  }
  const qpt = (a, c, b, t) => ({ x: (1 - t) * (1 - t) * a.x + 2 * (1 - t) * t * c.x + t * t * b.x,
                                 y: (1 - t) * (1 - t) * a.y + 2 * (1 - t) * t * c.y + t * t * b.y });

  // ── static layer: rings, edges, nodes, labels ───────────────────────────────────────
  const drawStatic = timed("static", drawStatic_);
  function drawStatic_() {
    if (!S.layout) layout();
    const td = tfData(); if (!td) return;
    const L = S.layout, g = S.staticCanvas.getContext("2d");
    g.setTransform(L.dpr, 0, 0, L.dpr, 0, 0);
    g.clearRect(0, 0, L.W, L.H);
    const fedPos = pos({ x: 0.5, y: 0.51 });
    g.strokeStyle = "rgba(140,160,190,0.07)"; g.lineWidth = 1;
    for (const rr of [1.7, 3.2, 4.9, 6.8]) { g.beginPath(); g.arc(fedPos.x, fedPos.y, L.R * rr, 0, Math.PI * 2); g.stroke(); }
    const nodes = td.nodes, byId = Object.fromEntries(nodes.map((n) => [n.id, n]));
    // edges
    for (const e of td.edges) {
      const a = byId[e.from], b = byId[e.to]; if (!a || !b) continue;
      const pa = pos(a), pb = pos(b), c = curve(pa, pb, 1);
      const grad = g.createLinearGradient(pa.x, pa.y, pb.x, pb.y);
      const al = 0.4 + 0.5 * Math.min(1, e.share * 4);              // even a minor flow stays readable
      grad.addColorStop(0, `rgba(255,77,109,${al})`); grad.addColorStop(1, `rgba(34,211,138,${al})`);
      g.strokeStyle = grad; g.lineWidth = 2 + 11 * e.share; g.lineCap = "round";
      g.beginPath(); g.moveTo(pa.x, pa.y); g.quadraticCurveTo(c.x, c.y, pb.x, pb.y); g.stroke();
    }
    // the Fed in the centre
    const fed = S.data.fed;
    g.save(); g.shadowColor = "rgba(242,184,75,0.35)"; g.shadowBlur = 24;
    g.fillStyle = "rgba(14,17,22,0.92)"; g.strokeStyle = "rgba(220,226,234,0.6)"; g.lineWidth = 1.4;
    g.beginPath(); g.arc(fedPos.x, fedPos.y, L.R * 1.25, 0, Math.PI * 2); g.fill(); g.stroke(); g.restore();
    g.textAlign = "center"; g.fillStyle = COL.text; g.font = `700 ${Math.round(L.R * 0.36)}px "Segoe UI", sans-serif`;
    g.fillText("THE FED", fedPos.x, fedPos.y - L.R * 0.08);
    g.fillStyle = COL.amber; g.font = `700 ${Math.round(L.R * 0.36)}px Consolas, monospace`;
    g.fillText(fed ? `${fed.days.toFixed(1)} days` : "—", fedPos.x, fedPos.y + L.R * 0.42);
    // nodes
    for (const n of nodes) drawNode(g, n, pos(n), n.id === S.hover);
    S.nodePos = nodes.map((n) => ({ id: n.id, ...pos(n) }));
    // an empty notable-stock slot: a faint placeholder that explains itself on hover (wide layout)
    const st = td.stocks;
    if (st && st.slots > 0 && !L.narrow && !nodes.some((n) => n.slot) && st.positions && st.positions[0]) {
      const p = pos({ x: st.positions[0][0], y: st.positions[0][1] });
      g.save(); g.setLineDash([4, 5]); g.strokeStyle = S.hover === "stock-empty" ? "rgba(200,210,225,0.7)" : "rgba(139,151,168,0.45)";
      g.lineWidth = 1.4; g.beginPath(); g.arc(p.x, p.y, L.R * 0.9, 0, Math.PI * 2); g.stroke(); g.restore();
      g.textAlign = "center"; g.fillStyle = COL.muted || "#8b97a8";
      g.font = `600 ${Math.round(L.R * 0.36)}px "Segoe UI", sans-serif`;
      g.fillText("NOTABLE STOCK", p.x, p.y + L.R * 1.45);
      g.font = `${Math.round(L.R * 0.34)}px "Segoe UI", sans-serif`;
      g.fillText(st.loading && st.loading.length ? `loading ${st.loading[0]}…` : "none stands out now", p.x, p.y + L.R * 1.95);
      S.nodePos.push({ id: "stock-empty", ...p });
    }
  }

  function drawNode(g, n, p, hot) {
    const L = S.layout, R = L.R * (hot ? 1.08 : 1);
    const col = nodeColor(n), mag = Math.min(4, Math.abs(n.z ?? 0));
    const dimmed = ["stale", "no_data", "warming_up"].includes(n.status);
    // glow
    if (n.z != null && mag > 0.3) {
      const gl = g.createRadialGradient(p.x, p.y, R * 0.6, p.x, p.y, R * (1.6 + mag * 0.35));
      gl.addColorStop(0, hexA(col, 0.30 + 0.12 * mag)); gl.addColorStop(1, hexA(col, 0));
      g.fillStyle = gl; g.beginPath(); g.arc(p.x, p.y, R * (1.6 + mag * 0.35), 0, Math.PI * 2); g.fill();
    }
    // sphere
    const body = g.createRadialGradient(p.x - R * 0.35, p.y - R * 0.45, R * 0.1, p.x, p.y, R);
    body.addColorStop(0, "#e9edf2"); body.addColorStop(0.35, "#8d96a2"); body.addColorStop(0.8, "#2c333c"); body.addColorStop(1, "#15191e");
    g.fillStyle = body; g.beginPath(); g.arc(p.x, p.y, R, 0, Math.PI * 2); g.fill();
    g.lineWidth = 2.4; g.strokeStyle = dimmed ? "rgba(139,151,168,0.6)" : col;
    if (dimmed) g.setLineDash([4, 4]);
    g.beginPath(); g.arc(p.x, p.y, R + 1, 0, Math.PI * 2); g.stroke(); g.setLineDash([]);
    // short name
    g.textAlign = "center"; g.fillStyle = "#0d1014"; g.font = `800 ${Math.round(R * 0.36)}px "Segoe UI", sans-serif`;
    g.fillText(n.short, p.x, p.y + R * 0.13);
    // label; then sigma, change and an inline sparkline on one line (keeps the block short)
    const fs = Math.round(Math.max(11, Math.min(14, R * 0.40)));
    g.fillStyle = COL.text; g.font = `600 ${fs}px "Segoe UI", sans-serif`;
    let label = S.layout.narrow ? n.label.replace(/\s*\(.*\)\s*$/, "") : n.label;   // "Stablecoins (cash in crypto)" -> "Stablecoins"
    if (!S.layout.narrow && g.measureText(label).width / 2 + 6 > roomBeside(n, p)) label = n.label.replace(/\s*\(.*\)\s*$/, "");
    const lw = g.measureText(label).width;
    const lx = Math.max(lw / 2 + 2, Math.min(S.layout.W - lw / 2 - 2, p.x));          // never clipped at the edges
    g.fillText(label, lx, p.y + R + fs + 3);
    const y2 = p.y + R + 2 * fs + 7;
    g.font = `600 ${fs - 1}px Consolas, monospace`;
    const zs = fmtZ(n.z), cs = fmtPct(n.change_pct);
    const hasSpark = !S.layout.narrow && n.spark && n.spark.length > 2, sw = hasSpark ? Math.round(fs * 3.4) : 0;
    const wz = g.measureText(zs).width, wc = g.measureText(cs).width, gap = 8;
    const x0 = p.x - (wz + wc + gap + (sw ? sw + gap : 0)) / 2;
    g.textAlign = "left";
    g.fillStyle = n.z == null ? COL.muted : nodeColor(n); g.fillText(zs, x0, y2);
    g.fillStyle = n.change_pct == null ? COL.muted : n.change_pct >= 0 ? COL.in : COL.out; g.fillText(cs, x0 + wz + gap, y2);
    if (hasSpark) sparkline(g, n.spark, x0 + wz + wc + 2 * gap, y2 - fs + 2, sw, fs - 1, nodeColor(n));
    // source / age badge, beside the node (left of it near the right edge)
    const age = n.age_s == null ? "" : fmtAge(n.age_s + Math.max(0, nowServer() - S.data.generated_ts));
    const lat = n.latency_ms == null ? "" : ` ${Math.round(n.latency_ms)}ms ·`;
    const btxt = `${n.source.short || n.source.id}${n.source.proxy ? "*" : ""}${lat} ${age}`;
    g.font = `600 10px Consolas, monospace`;
    let bw = g.measureText(btxt).width + 10, bx, by;
    if (S.layout.narrow) {                                       // phones: compact badge under the numbers
      const short = `${n.source.short || n.source.id}${n.source.proxy ? "*" : ""} ${age}`;
      bw = g.measureText(short).width + 10; bx = p.x - bw / 2; by = y2 + 17;
      drawBadge(g, n, short, bx, by, bw);
      return;
    }
    const need = R + 6 + bw + 4;                                 // room a side badge takes
    const fitsRight = p.x + R + 6 + bw < S.layout.W - 4 && gapTo(n, p, +1) > need + R;
    const fitsLeft = p.x - R - 6 - bw > 4 && gapTo(n, p, -1) > need + R;
    if (fitsRight || fitsLeft) {
      bx = fitsRight ? p.x + R + 6 : p.x - R - 6 - bw; by = p.y - R * 0.35;
    } else {                                                     // crowded row: badge under the numbers
      bx = p.x - bw / 2; by = y2 + 17;
    }
    drawBadge(g, n, btxt, bx, by, bw);
  }
  /* Distance to the nearest node on the same row on one side (+1 right, -1 left). */
  function gapTo(n, p, side) {
    const td = tfData(); if (!td) return Infinity;
    let gap = Infinity;
    for (const o of td.nodes) {
      if (o.id === n.id) continue;
      const q = pos(o), dx = (q.x - p.x) * side;
      if (dx > 0 && Math.abs(q.y - p.y) < S.layout.R * 2.5) gap = Math.min(gap, dx);
    }
    return gap;
  }
  /* Half-width available to a node's label before it reaches a neighbour on the same row. */
  function roomBeside(n, p) {
    const td = tfData(); if (!td) return Infinity;
    let room = Infinity;
    for (const o of td.nodes) {
      if (o.id === n.id) continue;
      const q = pos(o);
      if (Math.abs(q.y - p.y) < S.layout.R * 2.5) room = Math.min(room, Math.abs(q.x - p.x) - S.layout.R - 10);
    }
    return room;
  }
  function drawBadge(g, n, btxt, bx, by, bw) {
    const bcol = { live: "rgba(34,211,138,0.16)", delayed: "rgba(242,184,75,0.18)", closed: "rgba(139,151,168,0.18)" }[n.status] || "rgba(255,77,109,0.18)";
    const tcol = { live: "#79f0bd", delayed: COL.amber, closed: "#b3bdca" }[n.status] || "#ff8aa0";
    roundRect(g, bx, by - 11, bw, 15, 4); g.fillStyle = bcol; g.fill();
    g.fillStyle = tcol; g.textAlign = "left"; g.fillText(btxt, bx + 5, by);
  }
  function sparkline(g, xs, x, y, w, h, col) {
    const v = xs.filter((a) => a != null); if (v.length < 2) return;
    const lo = Math.min(...v, 0), hi = Math.max(...v, 0), span = hi - lo || 1;
    g.save(); g.globalAlpha = 0.85; g.strokeStyle = col; g.lineWidth = 1.2; g.beginPath();
    let first = true;
    xs.forEach((a, i) => { if (a == null) return; const px = x + (i / (xs.length - 1)) * w, py = y + h - ((a - lo) / span) * h;
      if (first) { g.moveTo(px, py); first = false; } else g.lineTo(px, py); });
    g.stroke();
    const zy = y + h - ((0 - lo) / span) * h;
    g.globalAlpha = 0.25; g.setLineDash([2, 3]); g.beginPath(); g.moveTo(x, zy); g.lineTo(x + w, zy); g.stroke(); g.restore();
  }
  function roundRect(g, x, y, w, h, r) { g.beginPath(); g.moveTo(x + r, y); g.arcTo(x + w, y, x + w, y + h, r); g.arcTo(x + w, y + h, x, y + h, r);
    g.arcTo(x, y + h, x, y, r); g.arcTo(x, y, x + w, y, r); g.closePath(); }
  function hexA(hex, a) { const n = parseInt(hex.slice(1), 16); return `rgba(${(n >> 16) & 255},${(n >> 8) & 255},${n & 255},${a})`; }

  // ── particles ───────────────────────────────────────────────────────────────────────
  function buildParticles() {
    const td = tfData(); if (!td || !S.layout) return;
    const byId = Object.fromEntries(td.nodes.map((n) => [n.id, n]));
    S.particles = [];
    for (const e of td.edges) {
      const a = byId[e.from], b = byId[e.to]; if (!a || !b) continue;
      const pa = pos(a), pb = pos(b), c = curve(pa, pb, 1);
      const count = 2 + Math.round(Math.min(1, e.share * 3) * 7), speed = 0.10 + 0.25 * Math.min(1, e.share * 3);
      for (let i = 0; i < count; i++) S.particles.push({ a: pa, b: pb, c, t0: i / count, speed, size: 1.6 + 2.2 * Math.min(1, e.share * 3) });
    }
  }
  function blit() {
    if (!S.layout) return;
    const L = S.layout;
    ctx.setTransform(1, 0, 0, 1, 0, 0); ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.drawImage(S.staticCanvas, 0, 0);
  }
  function loop(ts) {
    if (!S.motion || S.hidden) return;
    requestAnimationFrame(loop);
    if (ts && ts - S.lastFrame < 33) return;                       // <= 30 fps
    S.lastFrame = ts || performance.now();
    drawFrame();
  }
  const drawFrame = timed("frame", function () {
    blit();
    if (!S.particles.length) return;
    const L = S.layout, t = performance.now() / 1000;
    ctx.setTransform(L.dpr, 0, 0, L.dpr, 0, 0);
    for (const p of S.particles) {
      const u = (p.t0 + t * p.speed) % 1, q = qpt(p.a, p.c, p.b, u);
      const r = Math.round(255 * (1 - u) + 34 * u), gg = Math.round(77 * (1 - u) + 211 * u), bb = Math.round(109 * (1 - u) + 138 * u);
      ctx.fillStyle = `rgba(${r},${gg},${bb},0.35)`; ctx.beginPath(); ctx.arc(q.x, q.y, p.size * 2.2, 0, Math.PI * 2); ctx.fill();
      ctx.fillStyle = "rgba(255,255,255,0.92)"; ctx.beginPath(); ctx.arc(q.x, q.y, p.size, 0, Math.PI * 2); ctx.fill();
    }
  });

  // ── hover tooltip ───────────────────────────────────────────────────────────────────
  function pointerAt(ev) {
    const r = canvas.getBoundingClientRect(), x = ev.clientX - r.left, y = ev.clientY - r.top;
    let hit = null;
    for (const n of S.nodePos || []) if (Math.hypot(n.x - x, n.y - y) < S.layout.R + (ev.pointerType === "touch" ? 16 : 8)) hit = n.id;
    if (hit !== S.hover) { S.hover = hit; drawStatic(); if (!S.motion || S.hidden) blit(); }
    if (hit) showTip(hit, x, y); else $("tip").hidden = true;
  }
  canvas.addEventListener("mousemove", pointerAt);
  // touch screens have no hover: a tap on a node opens its card, a tap elsewhere closes it
  canvas.addEventListener("pointerdown", (ev) => { if (ev.pointerType !== "mouse") pointerAt(ev); });
  canvas.addEventListener("mouseleave", () => { S.hover = null; $("tip").hidden = true; drawStatic(); blit(); });

  function placeTip(tip, mx, my) {
    if (mx == null) return;
    const wrap = $("mapwrap").getBoundingClientRect();
    const tw = tip.offsetWidth, th = tip.offsetHeight;
    tip.style.left = Math.max(6, Math.min(wrap.width - tw - 6, mx + 18)) + "px";
    tip.style.top = Math.max(6, Math.min(wrap.height - th - 6, my - th / 2)) + "px";
  }
  function slotTip(td, mx, my) {                                   // the empty notable-stock slot
    const tip = $("tip"), st = td.stocks || {};
    let h = `<h3><span>Notable stock</span><span class="badge b-no_data">NONE NOW</span></h3>`;
    h += `<div class="note">${esc(st.note || "")}</div>`;
    h += `<div class="row"><span>Rule</span><span>|z| ≥ ${st.rule ? st.rule.z_min : "—"} and score ≥ ${st.rule ? st.rule.score_min : "—"} ` +
         `<span class="muted">(score = |z| × √volume ratio)</span></span></div>`;
    for (const c of (st.candidates || []).slice(0, 3))
      h += `<div class="row"><span>${esc(c.symbol)}</span><span><span class="${cls(c.z)}">${fmtPct(c.change_pct)} · ${fmtZ(c.z)}</span>` +
           ` <span class="muted">vol ${c.rvol == null ? "—" : c.rvol.toFixed(1) + "×"} · score ${c.score.toFixed(1)}</span></span></div>`;
    h += `<div class="row"><span>Screened</span><span>${st.screened || 0} of ${st.universe || 0} large caps` +
         `${st.scanned_at ? " · " + esc(new Date(st.scanned_at).toLocaleTimeString()) : ""}</span></div>`;
    tip.innerHTML = h; tip.hidden = false; placeTip(tip, mx, my);
  }
  function showTip(id, mx, my) {
    const tip = $("tip"), d = S.data; if (!d) return;
    if (id === "stock-empty") { const td = tfData(); if (td) slotTip(td, mx, my); return; }
    const cur = d.timeframes[S.tf].nodes.find((x) => x.id === id);
    // a notable-stock slot can hold a different stock in each window: match those by symbol, not slot id
    const same = (x) => cur && cur.slot ? x.slot && x.slot.symbol === cur.slot.symbol : x.id === id;
    const all = Object.fromEntries(Object.entries(d.timeframes).map(([k, v]) => [k, v.nodes.find(same)]));
    const n = all[S.tf]; if (!n) { tip.hidden = true; return; }
    const v = n.volume, dv = n.derivatives, src = n.source, ex = n.extra || {};
    let h = `<h3><span>${esc(n.label)}</span>${statusBadge(n.status)}</h3>`;
    if (n.slot) h += `<div class="row"><span>Notable stock · slot ${n.slot.index}</span><span>picked from ${n.slot.universe} large caps ` +
                     `<span class="muted">(screen: ${fmtZ(n.slot.screen.z)}, volume ${n.slot.screen.rvol == null ? "—" : n.slot.screen.rvol.toFixed(1) + "×"})</span></span></div>`;
    h += `<div class="row"><span>Price</span><span>${fmtPx(n.price)} <span class="muted">${esc(src.symbol)}</span></span></div>`;
    h += `<div class="row"><span>Source</span><span>${esc(src.name)}${src.proxy ? ' <span class="badge b-proxy">PROXY</span>' : ""}</span></div>`;
    h += `<div class="row"><span>What it is</span><span>${esc(src.description || "")}</span></div>`;
    h += `<div class="row"><span>Data age · latency</span><span>${fmtAge(n.age_s)} · ${n.latency_ms == null ? "—" : Math.round(n.latency_ms) + " ms"}</span></div>`;
    h += `<div class="tfrow">` + ["1h", "today", "1w"].map((t) => { const x = all[t];
      return `<div><b>${t === "today" ? "TODAY" : t.toUpperCase()}</b><span class="${cls(x && x.change_pct)}">${fmtPct(x && x.change_pct)}</span><br>` +
        `<span class="${cls(x && x.z)}">${fmtZ(x && x.z)}</span></div>`; }).join("") + `</div>`;
    {                                                              // its arrows in this window, or why it has none
      const td = d.timeframes[S.tf], names = Object.fromEntries(td.nodes.map((x) => [x.id, x.short || x.label]));
      const es = td.edges.filter((e) => e.from === id || e.to === id);
      if (es.length) h += `<div class="row"><span>Arrows</span><span>${es.map((e) => e.from === id
        ? `→ ${esc(names[e.to] || e.to)} <span class="muted">${e.weight.toFixed(2)}</span>`
        : `← ${esc(names[e.from] || e.from)} <span class="muted">${e.weight.toFixed(2)}</span>`).join(" · ")}</span></div>`;
      else if (n.no_edge) h += `<div class="row"><span>Arrows</span><span>${esc(n.no_edge.text)}</span></div>`;
    }
    if (n.history_from) h += `<div class="row"><span>History starts</span><span>${esc(new Date(n.history_from).toLocaleDateString(undefined, { day: "numeric", month: "short" }))} — too new for this window</span></div>`;
    h += sparkSvg(n.spark, nodeColor(n));
    h += `<div class="row"><span>Typical move (1σ) for this window</span><span>${n.sigma_pct == null ? "—" : n.sigma_pct.toFixed(2) + "%"}</span></div>`;
    h += `<div class="row"><span>σ estimated from</span><span>${esc(n.sigma_method || "—")}${n.sigma_days ? " on " + esc(n.sigma_days) : ""} (${n.sigma_samples ?? 0})</span></div>`;
    if (v) {
      h += `<div class="row"><span>Volume vs usual</span><span>${v.rvol == null ? "—" : v.rvol.toFixed(2) + "×"} <span class="muted">(${v.baseline_days} days)</span></span></div>`;
      if (v.net_taker != null) h += `<div class="row"><span>Net taker flow</span><span class="${cls(v.net_taker)}">${fmtUsd(v.net_taker)} <span class="muted">(${(v.taker_buy_ratio * 100).toFixed(1)}% buys)</span></span></div>`;
    }
    if (dv) {
      if (dv.funding_apr_pct != null) h += `<div class="row"><span>Perp funding</span><span>${dv.funding_apr_pct.toFixed(1)}% a year</span></div>`;
      if (dv.oi_change_pct != null) h += `<div class="row"><span>Open interest (window)</span><span class="${cls(dv.oi_change_pct)}">${fmtPct(dv.oi_change_pct)}</span></div>`;
    }
    for (const r of n.reference || []) {
      h += `<div class="row"><span>${esc(r.symbol)} (${esc(r.source_name)})</span><span>${fmtPx(r.price)} ${statusBadge(r.status)}` +
        `${r.basis_pct != null ? ` <span class="muted">basis ${fmtPct(r.basis_pct, 2)}</span>` : ""}</span></div>`;
    }
    if (ex.supply_usd) {
      h += `<div class="row"><span>Stablecoin supply</span><span>$${(ex.supply_usd / 1e9).toFixed(1)}B</span></div>`;
      h += `<div class="row"><span>Supply change 24h / 7d</span><span><span class="${cls(ex.supply_change_24h_usd)}">${fmtUsd(ex.supply_change_24h_usd)}</span> / <span class="${cls(ex.supply_change_7d_usd)}">${fmtUsd(ex.supply_change_7d_usd)}</span></span></div>`;
    }
    if (ex.pairs) h += `<div class="row"><span>FX legs</span><span class="muted">${Object.entries(ex.pairs).map(([k, x]) => k + " " + (x == null ? "—" : x.toFixed(4))).join(" · ")}</span></div>`;
    if (n.note) h += `<div class="note">${esc(n.note)}</div>`;
    tip.innerHTML = h; tip.hidden = false;
    placeTip(tip, mx, my);
  }

  function sparkSvg(xs, col) {
    const v = (xs || []).filter((a) => a != null);
    if (v.length < 3) return "";
    const w = 300, h = 46, lo = Math.min(...v, 0), hi = Math.max(...v, 0), span = hi - lo || 1;
    const pts = xs.map((a, i) => a == null ? null : `${(i / (xs.length - 1) * w).toFixed(1)},${(h - 3 - (a - lo) / span * (h - 6)).toFixed(1)}`)
      .filter(Boolean).join(" ");
    const zy = (h - 3 - (0 - lo) / span * (h - 6)).toFixed(1);
    return `<svg class="tipspark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" aria-label="move over the window">` +
      `<line x1="0" x2="${w}" y1="${zy}" y2="${zy}" stroke="#5b6777" stroke-dasharray="3 4" stroke-width="1"/>` +
      `<polyline points="${pts}" fill="none" stroke="${col}" stroke-width="1.8" vector-effect="non-scaling-stroke"/>` +
      `<text x="2" y="10" fill="#8592a3" font-size="9">${hi.toFixed(2)}%</text><text x="2" y="${h - 2}" fill="#8592a3" font-size="9">${lo.toFixed(2)}%</text></svg>`;
  }

  // ── right panel ─────────────────────────────────────────────────────────────────────
  const renderPanel = timed("panel", renderPanel_);
  function renderPanel_() {
    const d = S.data, td = tfData(); if (!td) return;
    $("windowLabel").textContent = `${td.label} · ${td.window.label}`;
    gauge(td.risk);
    tiles(td.tiles);
    fedPanel(d.fed, d.events);
    $("bullets").innerHTML = td.narrative.map((b) => `<li class="${esc(b.level)}" title="rule: ${esc(b.rule)}">${esc(b.text)}</li>`).join("");
    const names = Object.fromEntries(td.nodes.map((n) => [n.id, n.label]));
    $("flows").innerHTML = td.edges.length ? td.edges.slice(0, 7).map((e) =>
      `<div class="flow"><div><div>${esc(names[e.from])} → ${esc(names[e.to])}</div><div class="bar" style="width:${Math.max(6, e.share * 100 * 2.2).toFixed(0)}%"></div></div>` +
      `<div class="w">${e.weight.toFixed(2)}${e.corr == null ? "" : "<br>ρ " + e.corr.toFixed(2)}</div></div>`).join("")
      : `<div class="muted">No asset pair moved clearly in opposite directions in this window.</div>`;
    $("statusStrip").innerHTML = d.sources.map((s) => {
      const k = ["connected", "ok"].includes(s.state) ? "ok" : s.state === "disabled" ? "" : ["error", "reconnecting"].includes(s.state) ? "err" : "warn";
      const lat = s.latency_ms_median != null ? ` ${Math.round(s.latency_ms_median)}ms` : "";
      const rate = s.msgs_per_s != null && s.kind === "stream" ? ` ${s.msgs_per_s}/s` : "";
      return `<span class="pill ${k}" title="${esc(s.title)}: ${esc(s.state)} ${esc(s.detail || "")} ${esc(s.last_error || "")}"><i></i>${esc(s.id)}${lat}${rate}</span>`;
    }).join("");
    const m = d.market, c = d.meta.clock;
    $("footClock").textContent = `updated ${new Date(d.generated_ts * 1000).toLocaleTimeString()} · PC clock corrected by ${c.offset_ms >= 0 ? "+" : ""}${Math.round(c.offset_ms)} ms (NTP) · US stocks: ${m.us_equities} · FX: ${m.fx} · VIX: ${m.vix}` +
      (d.meta.process && d.meta.process.cpu_percent != null ? ` · service CPU ${d.meta.process.cpu_percent}% · RAM ${d.meta.process.rss_mb} MB` : "");
  }

  function gauge(r) {
    const svg = $("gauge"), cx = 110, cy = 112, R = 92;
    const ang = (s) => Math.PI * (1 - (s + 100) / 200);
    const arc = (s0, s1, col) => { const a0 = ang(s0), a1 = ang(s1);
      return `<path d="M ${cx + R * Math.cos(a0)} ${cy - R * Math.sin(a0)} A ${R} ${R} 0 0 1 ${cx + R * Math.cos(a1)} ${cy - R * Math.sin(a1)}" stroke="${col}" stroke-width="12" fill="none" stroke-linecap="butt"/>`; };
    let s = arc(-100, -60, COL.out) + arc(-60, -20, "#e0646f") + arc(-20, 20, COL.amber) + arc(20, 60, "#7fd88f") + arc(60, 100, COL.in);
    const score = r && r.score != null ? r.score : null;
    if (score != null) {
      const a = ang(score), nx = cx + (R - 22) * Math.cos(a), ny = cy - (R - 22) * Math.sin(a);
      s += `<line x1="${cx}" y1="${cy}" x2="${nx.toFixed(1)}" y2="${ny.toFixed(1)}" stroke="#e8edf3" stroke-width="3" stroke-linecap="round"/><circle cx="${cx}" cy="${cy}" r="6" fill="#e8edf3"/>`;
      const col = score <= -20 ? COL.out : score >= 20 ? COL.in : COL.amber;
      const halo = `paint-order="stroke" stroke="#0c1015" stroke-width="5" stroke-linejoin="round"`;
      s += `<text x="${cx}" y="${cy - 58}" text-anchor="middle" fill="${col}" font-size="11.5" font-weight="700" letter-spacing="1.5" font-family="Segoe UI, sans-serif" ${halo}>${esc(r.label)}</text>`;
      s += `<text x="${cx}" y="${cy - 30}" text-anchor="middle" fill="${col}" font-size="26" font-weight="800" font-family="Consolas, monospace" ${halo}>${score > 0 ? "+" : score < 0 ? "−" : ""}${Math.abs(score)}</text>`;
    } else s += `<text x="${cx}" y="${cy - 34}" text-anchor="middle" fill="${COL.muted}" font-size="14">waiting for data</text>`;
    svg.innerHTML = s;
    if (r && r.components) {
      const top = r.components.filter((c) => c.contribution != null).sort((a, b) => Math.abs(b.contribution) - Math.abs(a.contribution)).slice(0, 3);
      $("gaugeFoot").innerHTML = `S = ${r.s == null ? "—" : (r.s >= 0 ? "+" : "−") + Math.abs(r.s).toFixed(2) + "σ"} · ${Math.round((r.coverage || 0) * 100)}% of inputs live<br>` +
        top.map((c) => `<span title="${esc(c.label)}">${esc(c.id.replace(/_/g, " "))} <b class="${cls(c.contribution)}">${c.contribution >= 0 ? "+" : "−"}${Math.abs(c.contribution).toFixed(2)}</b></span>`).join(" · ");
    }
  }

  function tiles(t) {
    const v = t.vix || {};
    const vp = v.proxy || {};
    $("tileVix").innerHTML = `<div class="th"><span>FEAR · VIX</span>${statusBadge(v.status || "no_data")}</div>` +
      `<div class="tv">${v.level == null ? "—" : v.level.toFixed(2)}<small class="${cls(v.change_pct)}">${fmtPct(v.change_pct)}</small></div>` +
      `<div class="ts" title="${esc(v.source || "")}">${v.quote_time ? "quote " + fmtAge(v.age_s) + " old (15-min delay)" : "no quote"}</div>` +
      `<div class="ts">live proxy UVXY perp <span class="${cls(vp.change_pct)}">${fmtPct(vp.change_pct)}</span> ${fmtZ(vp.z)}</div>`;
    const x = t.dxy || {};
    $("tileDxy").innerHTML = `<div class="th"><span>DOLLAR · DXY</span>${statusBadge(x.status || "no_data")}</div>` +
      `<div class="tv">${x.level == null ? "—" : x.level.toFixed(2)}<small class="${cls(x.change_pct)}">${fmtPct(x.change_pct)}</small></div>` +
      `<div class="ts">computed from 6 FX pairs · ${fmtAge(x.age_s)}</div><div class="ts">${fmtZ(x.z)} vs its usual move</div>`;
    const y = t.us10y;
    $("tile10y").innerHTML = y ? `<div class="th"><span>US 10Y YIELD</span>${statusBadge("daily")}</div>` +
      `<div class="tv">${y.value.toFixed(2)}%<small class="${cls(-(y.change_bp || 0))}">${y.change_bp == null ? "" : (y.change_bp > 0 ? "+" : "") + y.change_bp + "bp"}</small></div>` +
      `<div class="ts">FRED close of ${esc(y.date)} · 5d ${y.change_5d_bp > 0 ? "+" : ""}${y.change_5d_bp ?? "—"}bp</div>` +
      (y.live_hint ? `<div class="ts">live: TLT <span class="${cls(y.live_hint.change_pct)}">${fmtPct(y.live_hint.change_pct)}</span> (bonds up = yields down)</div>` : "")
      : `<div class="th"><span>US 10Y YIELD</span></div><div class="ts">needs FRED_API_KEY</div>`;
    const l = t.crypto_leverage;
    $("tileLev").innerHTML = l ? `<div class="th"><span>BTC LEVERAGE</span>${statusBadge("live")}</div>` +
      `<div class="tv">${l.oi_change_pct == null ? "—" : fmtPct(l.oi_change_pct)}<small class="muted">OI</small></div>` +
      `<div class="ts">funding ${l.funding_apr_pct == null ? "—" : l.funding_apr_pct.toFixed(1) + "% a year"}</div>` +
      `<div class="ts">${t.etf_flows && t.etf_flows.btc ? "spot ETFs " + fmtUsd(t.etf_flows.btc.last_total_musd * 1e6) + " (" + esc(t.etf_flows.btc.last_date) + ")" : "Binance perpetual"}</div>`
      : `<div class="th"><span>BTC LEVERAGE</span></div><div class="ts">waiting for data</div>`;
  }

  function fedPanel(fed, events) {
    if (!fed) { $("fedCount").textContent = "—"; $("fedWhen").textContent = "FOMC calendar not loaded yet"; return; }
    S.fed = fed;
    $("fedWhen").innerHTML = `${esc(fed.when_ny)} · <b>${esc(fed.when_local)}</b> ${esc(localTzLabel())}<br><span class="muted">${esc(fed.label)} · press conference 2:30 PM New York</span>`;
    const ev = (events || []).slice(0, 4);
    $("fedEvents").innerHTML = ev.length ? "<div style='color:#c4ccd6'>Scheduled high-impact data:</div>" + ev.map((e) =>
      `<div>${esc(e.country)} · ${esc(e.title)} · ${esc(e.when_text)}</div>`).join("") : "";
    tickFed();
  }
  function localTzLabel() { return (S.fed && S.fed.local_label) || "local"; }
  function tickFed() {
    if (!S.fed) return;
    let s = Math.max(0, S.fed.decision_ts - nowServer());
    const d = Math.floor(s / 86400); s -= d * 86400; const h = Math.floor(s / 3600); s -= h * 3600; const m = Math.floor(s / 60), sec = Math.floor(s - m * 60);
    $("fedCount").textContent = `${d}d ${String(h).padStart(2, "0")}h ${String(m).padStart(2, "0")}m ${String(sec).padStart(2, "0")}s`;
  }
  setInterval(() => { if (!S.hidden) tickFed(); }, 1000);

  // ── news wire ───────────────────────────────────────────────────────────────────────
  function renderWire() {
    const d = S.data, td = tfData(); if (!d) return;
    const items = [];
    const flowLines = (td ? td.narrative : []).filter((b) => ["crypto_to_cash", "bonds_bid_safety", "strongest_flow", "dollar_haven", "fear_spike", "broad_liquidation"].includes(b.id)).slice(0, 2);
    for (const b of flowLines) items.push(`<span class="wi"><span class="tag flow">MONEY FLOW</span><span>${esc(b.text)}</span></span>`);
    for (const n of d.news.slice(0, 22)) {
      const tag = n.tags && n.tags[0] ? `<span class="tag">${esc(n.tags[0])}</span>` : "";
      const title = n.url ? `<a href="${esc(n.url)}" target="_blank" rel="noopener noreferrer">${esc(n.title)}</a>` : esc(n.title);
      items.push(`<span class="wi">${tag}${title}<span class="src">${esc(n.source)} · ${fmtAge(n.age_s)} ago</span></span>`);
    }
    const inner = $("wireInner");
    if (!items.length) { inner.innerHTML = `<span class="muted">waiting for headlines…</span>`; return; }
    const one = items.join("");
    inner.innerHTML = `<div class="wire-copy">${one}</div><div class="wire-copy" aria-hidden="true">${one}</div>`;
    const w = (inner.scrollWidth || 4000) / 2;
    inner.style.animationDuration = Math.max(40, w / 70) + "s";            // ~70 px per second
    S.wirePending = false;
  }
  $("wireInner").addEventListener("animationiteration", () => { if (S.wirePending && !S.hidden) renderWire(); });

  // read-only hooks for tests and the console (no effect on the page)
  window.__mfDebug = { nodePos: () => S.nodePos, layout: () => S.layout, tf: () => S.tf, motion: () => S.motion,
    particles: () => S.particles.length, generated: () => S.data && S.data.generated_at,
    render: () => { S.lastPanel = 0; render(); }, frame: () => drawFrame() };   // for soak tests of the drawing code

  // ── boot ────────────────────────────────────────────────────────────────────────────
  let resizeTimer = null;
  window.addEventListener("resize", () => { clearTimeout(resizeTimer); resizeTimer = setTimeout(() => { layout(); drawStatic(); buildParticles(); blit(); }, 120); });
  layout();
  connect();
  if (S.motion) requestAnimationFrame(loop);
})();
