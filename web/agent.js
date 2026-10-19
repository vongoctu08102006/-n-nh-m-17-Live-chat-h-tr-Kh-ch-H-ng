/* Dashboard nhan vien.
 * - Hang cho realtime qua WebSocket /ws-agents (queue_snapshot / queue_add / queue_remove)
 * - "Nhan" khach = noi WebSocket /ws/{id}?role=agent (server dung LWT: chi 1 nhan vien thang)
 * - Chat song song nhieu khach: moi cuoc chat 1 WebSocket rieng, co huy hieu tin chua doc
 * - Vao lai trang: tu noi lai cac cuoc chat dang phu trach (qua API Q4 /api/agents/{id}/conversations)
 * Chong XSS: moi noi dung dua vao DOM bang textContent. */
(function () {
  "use strict";
  const API = location.origin, WS_BASE = API.replace(/^http/, "ws");
  const PING_MS = 25000;                                   // presence Redis het han sau 90s
  const FATAL = new Set([4400, 4403, 4404, 4409, 4410]);   // ma dong tu server -> khong ket noi lai
  const QUICK = [
    "Xin chào, mình có thể giúp gì cho bạn?",
    "Bạn vui lòng cho mình xin mã đơn hàng nhé.",
    "Mình đang kiểm tra, bạn chờ một chút nhé.",
    "Cảm ơn bạn đã liên hệ, chúc bạn một ngày tốt lành!"
  ];

  let agentKey = "";                                       // ma nhan vien (neu server dat AGENT_KEY)
  const $ = (s) => document.querySelector(s);
  const enc = encodeURIComponent;
  function el(tag, cls, text) { const n = document.createElement(tag); if (cls) n.className = cls; if (text != null) n.textContent = text; return n; }
  function api(path, opts) {
    opts = opts || {};
    if (agentKey) opts.headers = Object.assign({ "X-Agent-Key": agentKey }, opts.headers || {});
    return fetch(API + path, opts).then((r) => { if (!r.ok) { const e = new Error("HTTP " + r.status); e.status = r.status; throw e; } return r.json(); });
  }
  function hhmm(iso) { const d = new Date(iso); return isNaN(d) ? "" : d.toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit" }); }
  function dmy(iso) { const d = new Date(iso); return isNaN(d) ? "" : d.toLocaleDateString("vi-VN") + " " + hhmm(iso); }
  function ago(iso) {
    const m = Math.floor((Date.now() - new Date(iso).getTime()) / 60000);
    if (isNaN(m) || m < 1) return "vừa xong";
    if (m < 60) return m + " phút";
    return Math.floor(m / 60) + " giờ " + (m % 60) + " phút";
  }
  function ss(k, v) { try { if (v === undefined) return sessionStorage.getItem(k); if (v === null) sessionStorage.removeItem(k); else sessionStorage.setItem(k, v); } catch (e) { /* bo qua */ } return null; }

  // ---------------- trang thai ----------------
  let agentId = null, selected = null, queue = [], feed = null, feedRetry = 0, feedTimer = null, feedPing = null, server = null, toastTimer = null;
  const convs = {};   // id -> C

  function newConv(o) {
    return { id: o.id, customer_id: o.customer_id, status: o.status || "active", created_at: o.created_at || null,
      live: false, claiming: false, ws: null, joined: false, msgs: [], seen: {}, pending: [], loadedOnce: false,
      historyLoaded: false, nextBefore: null, unread: 0, customerOnline: false, retry: 0, pingTimer: null,
      reconnectTimer: null, prev: null, reconnecting: false, last: o.created_at || "" };
  }

  // ---------------- thong bao nho ----------------
  function toast(text) {
    let t = $("#toast"); if (!t) { t = el("div", "toast"); t.id = "toast"; document.body.appendChild(t); }
    t.textContent = text; t.style.display = "block"; clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { t.style.display = "none"; }, 4000);
  }
  function updateTitle() {
    const n = Object.values(convs).reduce((s, c) => s + c.unread, 0);
    document.title = (n ? "(" + n + ") " : "") + "Dashboard nhân viên";
  }

  // ---------------- dang nhap ----------------
  function login(name, key) {
    name = String(name || "").trim();
    agentKey = String(key || "").trim(); ss("lc_key", agentKey || null);
    if (!/^[A-Za-z0-9_.-]{1,32}$/.test(name)) { $("#login-err").textContent = "Chỉ dùng chữ không dấu, số, _ . - (tối đa 32 ký tự)"; return; }
    agentId = name; ss("lc_agent", name);
    $("#login").classList.add("hidden"); $("#app").classList.remove("hidden");
    $("#who").textContent = "👤 " + agentId;
    buildQuick(); connectFeed(); loadMine(); renderAll();
    setInterval(renderQueue, 30000);
  }
  $("#login-btn").onclick = () => login($("#login-name").value, $("#login-key").value);
  ["#login-name", "#login-key"].forEach((s) => $(s).addEventListener("keydown", (e) => { if (e.key === "Enter") login($("#login-name").value, $("#login-key").value); }));
  $("#logout").onclick = () => { ss("lc_agent", null); ss("lc_key", null); location.href = location.pathname; };

  // ---------------- hang cho realtime ----------------
  function setFeedStatus(on, text) { $("#feed-dot").className = "dot " + (on ? "on" : "warn"); $("#feed-text").textContent = text; }

  function connectFeed() {
    clearTimeout(feedTimer);
    const ws = new WebSocket(WS_BASE + "/ws-agents?agent_id=" + enc(agentId) + "&key=" + enc(agentKey));
    feed = ws;
    ws.onopen = () => { clearInterval(feedPing); feedPing = setInterval(() => { if (ws.readyState === 1) ws.send('{"type":"ping"}'); }, PING_MS); };
    ws.onmessage = (ev) => {
      let d; try { d = JSON.parse(ev.data); } catch (e) { return; }
      if (d.type === "queue_snapshot") {
        queue = d.queue; server = d.server_id; feedRetry = 0; setFeedStatus(true, "Trực tuyến · server " + server);
      } else if (d.type === "queue_add") {
        const c = d.conversation;
        if (!queue.some((q) => q.id === c.id) && !(convs[c.id] && convs[c.id].live)) queue.push({ id: c.id, customer_id: c.customer_id, created_at: c.created_at });
      } else if (d.type === "queue_remove") {
        queue = queue.filter((q) => q.id !== d.conversation_id);
      }
      renderQueue();
    };
    ws.onclose = (ev) => {
      clearInterval(feedPing);
      if (feed !== ws) return;
      if (ev.code === 4401) { setFeedStatus(false, "Sai mã nhân viên - đăng xuất và nhập lại"); return; }
      setFeedStatus(false, "Mất kết nối, đang thử lại…");
      feedTimer = setTimeout(connectFeed, Math.min(1000 * Math.pow(2, feedRetry++), 15000));
    };
  }

  // ---------------- nap cac cuoc chat cua toi ----------------
  function loadMine() {
    api("/api/agents/" + enc(agentId) + "/conversations?limit=30")
      .then((list) => Promise.all(list.map((x) => api("/api/conversations/" + x.id).catch(() => null))))
      .then((arr) => {
        arr.filter(Boolean).forEach((cv) => {
          if (convs[cv.id]) return;
          const C = newConv(cv);
          convs[cv.id] = C;
          if (cv.status === "active" && cv.agent_id === agentId) { C.live = true; connectConv(C); }
        });
        renderAll();
      }).catch(() => toast("Không tải được danh sách cuộc chat của bạn"));
  }

  // ---------------- nhan khach ----------------
  function claim(id) {
    if (convs[id] && convs[id].live) return select(id);
    const q = queue.find((x) => x.id === id);
    const C = newConv({ id, customer_id: q ? q.customer_id : "?", status: "waiting", created_at: q ? q.created_at : null });
    C.live = true; C.claiming = true; convs[id] = C;
    connectConv(C); renderQueue();
  }

  function dropConv(C) {
    clearInterval(C.pingTimer); clearTimeout(C.reconnectTimer);
    delete convs[C.id]; queue = queue.filter((q) => q.id !== C.id);
    if (selected === C.id) { selected = null; document.body.classList.remove("show-chat"); }
    renderAll();
  }

  // ---------------- ket noi 1 cuoc chat ----------------
  function connectConv(C) {
    clearTimeout(C.reconnectTimer);
    const ws = new WebSocket(WS_BASE + "/ws/" + C.id + "?role=agent&user_id=" + enc(agentId) + "&key=" + enc(agentKey));
    C.ws = ws; C.joined = false; C.historyLoaded = false; C.pending = [];
    ws.onopen = () => { clearInterval(C.pingTimer); C.pingTimer = setInterval(() => { if (ws.readyState === 1) ws.send('{"type":"ping"}'); }, PING_MS); };
    ws.onmessage = (ev) => {
      let d; try { d = JSON.parse(ev.data); } catch (e) { return; }
      if (d.type === "joined") {
        const cv = d.conversation;
        C.status = cv.status; C.customer_id = cv.customer_id; C.created_at = cv.created_at; C.joined = true; C.retry = 0; C.reconnecting = false;
        queue = queue.filter((q) => q.id !== C.id);
        loadHistory(C).catch(() => { C.historyLoaded = true; toast("Không tải được lịch sử tin nhắn"); }).then(() => { renderAll(); });
        api("/api/presence/customer/" + enc(C.customer_id)).then((p) => { C.customerOnline = p.online; renderAll(); }).catch(() => {});
        if (C.claiming) { C.claiming = false; select(C.id); }
        renderAll();
      } else if (d.type === "message") {
        if (!C.historyLoaded) C.pending.push(d.message); else { addMsg(C, d.message, true); renderMine(); updateTitle(); }
      } else if (d.type === "presence" && d.role === "customer") {
        C.customerOnline = d.event === "join";
        if (selected === C.id) sysLine(C.customerOnline ? "Khách đã quay lại" : "Khách đã rời khỏi cuộc trò chuyện");
        renderAll();
      } else if (d.type === "closed") {
        markClosed(C);
      } else if (d.type === "error") {
        toast(d.detail || "Có lỗi xảy ra");
      }
    };
    ws.onclose = (ev) => {
      clearInterval(C.pingTimer);
      if (C.ws !== ws || C.status === "closed") return;
      C.joined = false;
      if (ev.code === 4401) { toast("Sai mã nhân viên - không vào được cuộc chat"); return dropConv(C); }
      if (ev.code === 4409) { toast("Khách này đã được nhân viên khác nhận"); return dropConv(C); }
      if (ev.code === 4410) { toast("Cuộc trò chuyện đã kết thúc"); return markClosed(C); }
      if (ev.code === 4404 || ev.code === 4403 || ev.code === 4400) { toast("Không thể vào cuộc trò chuyện này (mã " + ev.code + ")"); return dropConv(C); }
      C.reconnecting = true; renderAll();
      C.reconnectTimer = setTimeout(() => connectConv(C), Math.min(1000 * Math.pow(2, C.retry++), 15000));
    };
  }

  function markClosed(C) {
    if (C.status === "closed" && !C.live) return;
    C.status = "closed"; C.live = false; C.joined = false; C.reconnecting = false;
    clearInterval(C.pingTimer); clearTimeout(C.reconnectTimer);
    if (C.ws) { const w = C.ws; C.ws = null; try { w.close(); } catch (e) { /* bo qua */ } }
    if (selected === C.id) sysLine("Cuộc trò chuyện đã kết thúc");
    renderAll();
  }

  // ---------------- tin nhan ----------------
  function addMsg(C, m, live) {
    if (C.seen[m.id]) return false;
    C.seen[m.id] = 1; C.msgs.push(m); C.last = m.sent_at;
    if (selected === C.id) {
      const box = $("#msgs"), stick = nearBottom();
      box.appendChild(bubble(m));
      if (stick || m.sender_type === "agent") toBottom();
    }
    if (live && m.sender_type === "customer" && (selected !== C.id || document.hidden)) C.unread++;
    return true;
  }

  function loadHistory(C) {
    return api("/api/conversations/" + C.id + "/messages?limit=50").then((d) => {
      d.messages.filter((m) => !C.seen[m.id]).reverse().forEach((m) => { C.seen[m.id] = 1; C.msgs.push(m); C.last = m.sent_at; });
      if (!C.loadedOnce) { C.nextBefore = d.next_before; C.loadedOnce = true; }
      C.historyLoaded = true;
      const p = C.pending; C.pending = []; p.forEach((m) => addMsg(C, m, true));
      if (selected === C.id) renderMessages();
    });
  }

  function loadOlder(C) {
    if (!C.nextBefore) return;
    const keep = $("#msgs").scrollHeight;
    api("/api/conversations/" + C.id + "/messages?limit=50&before=" + C.nextBefore).then((d) => {
      const older = d.messages.filter((m) => !C.seen[m.id]).reverse();
      older.forEach((m) => { C.seen[m.id] = 1; });
      C.msgs = older.concat(C.msgs); C.nextBefore = d.next_before;
      if (selected === C.id) { renderMessages(true); $("#msgs").scrollTop = $("#msgs").scrollHeight - keep; }
    }).catch(() => toast("Không tải được tin cũ"));
  }

  function send() {
    const C = convs[selected], input = $("#input"), text = input.value.trim();
    if (!C || !text || !C.ws || C.ws.readyState !== 1 || !C.historyLoaded || C.status === "closed") return;
    C.ws.send(JSON.stringify({ type: "message", content: text }));
    input.value = ""; input.style.height = "auto";
  }

  function closeConv() {
    const C = convs[selected]; if (!C) return;
    api("/api/conversations/" + C.id + "/close", { method: "POST" }).then(() => markClosed(C)).catch(() => toast("Không kết thúc được cuộc chat"));
  }

  // ---------------- chon / xem ----------------
  function select(id) {
    selected = id;
    const C = convs[id]; if (!C) return;
    C.unread = 0; document.body.classList.add("show-chat");
    if (!C.live && !C.historyLoaded) loadHistory(C).catch(() => toast("Không tải được lịch sử")).then(renderAll);
    if (!C.prev) { C.prev = []; loadPrev(C); }
    renderAll(); renderMessages(); toBottom();
    if (!$("#input").disabled) $("#input").focus();
  }

  function openReadonly(id, customer, status, created) {
    if (!convs[id]) convs[id] = newConv({ id, customer_id: customer, status, created_at: created });
    select(id);
  }

  function loadPrev(C) {
    api("/api/customers/" + enc(C.customer_id) + "/conversations?limit=10")
      .then((list) => { C.prev = list.filter((x) => x.id !== C.id); if (selected === C.id) renderInfo(); })
      .catch(() => { C.prev = []; });
  }

  // ---------------- ve giao dien ----------------
  function nearBottom() { const b = $("#msgs"); return b.scrollHeight - b.scrollTop - b.clientHeight < 80; }
  function toBottom() { const b = $("#msgs"); b.scrollTop = b.scrollHeight; }
  function sysLine(t) { const b = $("#msgs"); b.appendChild(el("div", "sys", t)); toBottom(); }

  function bubble(m) {
    const mine = m.sender_type === "agent";
    const n = el("div", "msg " + (mine ? "me" : "them"), m.content);
    n.appendChild(el("small", null, (mine && m.sender_id !== agentId ? m.sender_id + " · " : "") + hhmm(m.sent_at)));
    return n;
  }

  function renderMessages(keepScroll) {
    const C = convs[selected], box = $("#msgs"); box.textContent = "";
    if (!C) return;
    if (C.nextBefore) { const b = el("button", "more", "Xem tin cũ hơn"); b.onclick = () => loadOlder(C); box.appendChild(b); }
    C.msgs.forEach((m) => box.appendChild(bubble(m)));
    if (!keepScroll) toBottom();
  }

  function renderQueue() {
    const box = $("#queue"); box.textContent = ""; $("#queue-count").textContent = queue.length;
    if (!queue.length) { box.appendChild(el("div", "empty", "Không có khách đang chờ")); return; }
    queue.slice().sort((a, b) => String(a.created_at).localeCompare(String(b.created_at))).forEach((q) => {
      const row = el("div", "item"), main = el("div", "item-main");
      main.appendChild(el("div", "item-title", q.customer_id)); main.appendChild(el("div", "item-sub", "Chờ " + ago(q.created_at)));
      const busy = convs[q.id] && convs[q.id].claiming;
      const btn = el("button", "btn small", busy ? "Đang nhận…" : "Nhận"); btn.disabled = !!busy; btn.onclick = (e) => { e.stopPropagation(); claim(q.id); };
      row.appendChild(main); row.appendChild(btn); box.appendChild(row);
    });
  }

  function convRow(C, withBadge) {
    const row = el("div", "item" + (selected === C.id ? " sel" : "")), main = el("div", "item-main");
    const t = el("div", "item-title"); const d = el("span", "dot" + (C.live && C.customerOnline ? " on" : "")); t.appendChild(d); t.appendChild(el("span", null, C.customer_id));
    const last = C.msgs.length ? C.msgs[C.msgs.length - 1].content : (C.live ? "Chưa có tin nhắn" : dmy(C.created_at));
    main.appendChild(t); main.appendChild(el("div", "item-sub", last));
    row.appendChild(main);
    if (withBadge && C.unread) row.appendChild(el("span", "badge", C.unread > 9 ? "9+" : String(C.unread)));
    row.onclick = () => select(C.id);
    return row;
  }

  function renderMine() {
    const box = $("#mine"); box.textContent = "";
    const mine = Object.values(convs).filter((c) => c.live && c.status !== "closed").sort((a, b) => String(b.last).localeCompare(String(a.last)));
    $("#mine-count").textContent = mine.length;
    if (!mine.length) box.appendChild(el("div", "empty", "Chưa nhận khách nào"));
    mine.forEach((c) => box.appendChild(convRow(c, true)));
  }

  function renderHistory() {
    const box = $("#history"); box.textContent = "";
    const hist = Object.values(convs).filter((c) => c.status === "closed").sort((a, b) => String(b.created_at).localeCompare(String(a.created_at)));
    $("#hist-count").textContent = hist.length;
    if (!hist.length) box.appendChild(el("div", "empty", "Chưa có cuộc chat nào kết thúc"));
    hist.forEach((c) => box.appendChild(convRow(c, false)));
  }

  function renderChatHeader() {
    const C = convs[selected];
    $("#placeholder").classList.toggle("hidden", !!C);
    ["#chat-h", "#msgs", "#quick", "#composer"].forEach((s) => $(s).classList.toggle("hidden", !C));
    if (!C) return;
    $("#c-title").textContent = C.customer_id;
    const live = C.live && C.status !== "closed";
    $("#c-dot").className = "dot" + (live && C.customerOnline ? " on" : "");
    $("#c-sub").textContent = C.status === "closed" ? "Đã kết thúc · " + dmy(C.created_at)
      : live ? (C.customerOnline ? "Khách đang trực tuyến" : "Khách đang ngoại tuyến") : "Chỉ xem";
    $("#close-btn").classList.toggle("hidden", !live);
    const bn = $("#banner"); bn.classList.toggle("hidden", !(live && C.reconnecting));
    bn.textContent = "Mất kết nối, đang thử lại…";
    const can = live && C.joined && C.historyLoaded;
    $("#input").disabled = !can; $("#send").disabled = !can;
    $("#quick").classList.toggle("hidden", !live);
    $("#composer").classList.toggle("hidden", !live);
  }

  function renderInfo() {
    const box = $("#right"); box.textContent = "";
    const C = convs[selected]; if (!C) return;
    box.appendChild(el("h3", null, "Thông tin khách"));
    [["Mã khách", C.customer_id], ["Bắt đầu", dmy(C.created_at)], ["Trạng thái", C.status === "closed" ? "Đã kết thúc" : C.live ? "Đang chat" : C.status],
     ["Online", C.live ? (C.customerOnline ? "Có" : "Không") : "-"]].forEach((kv) => {
      const r = el("div", "kv"); r.appendChild(el("span", null, kv[0])); r.appendChild(el("span", null, kv[1])); box.appendChild(r);
    });
    const prev = el("div", "prev"); prev.appendChild(el("h3", null, "Các cuộc chat trước"));
    if (!C.prev || !C.prev.length) prev.appendChild(el("div", "empty", C.prev ? "Không có" : "Đang tải…"));
    else C.prev.forEach((p) => {
      const row = el("div", "item"), main = el("div", "item-main");
      main.appendChild(el("div", "item-title", dmy(p.created_at)));
      row.appendChild(main); row.appendChild(el("span", "chip-st " + p.status, p.status === "closed" ? "kết thúc" : p.status === "active" ? "đang chat" : "đang chờ"));
      row.onclick = () => openReadonly(p.id, C.customer_id, p.status, p.created_at);
      prev.appendChild(row);
    });
    box.appendChild(prev);
  }

  function renderAll() { renderQueue(); renderMine(); renderHistory(); renderChatHeader(); renderInfo(); updateTitle(); }

  function buildQuick() {
    const q = $("#quick"); q.textContent = "";
    QUICK.forEach((t) => { const b = el("button", "chip", t.length > 34 ? t.slice(0, 32) + "…" : t); b.title = t; b.onclick = () => { const i = $("#input"); if (!i.disabled) { i.value = t; i.focus(); } }; q.appendChild(b); });
  }

  // ---------------- su kien ----------------
  $("#send").onclick = send;
  $("#close-btn").onclick = function () {
    const b = this;
    if (!b.dataset.armed) { b.dataset.armed = "1"; b.textContent = "Bấm lại để xác nhận"; setTimeout(() => { delete b.dataset.armed; b.textContent = "Kết thúc chat"; }, 3000); return; }
    delete b.dataset.armed; b.textContent = "Kết thúc chat"; closeConv();
  };
  $("#back").onclick = () => document.body.classList.remove("show-chat");
  $("#input").addEventListener("keydown", (e) => { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); } });
  $("#input").addEventListener("input", (e) => { e.target.style.height = "auto"; e.target.style.height = Math.min(e.target.scrollHeight, 110) + "px"; });
  document.addEventListener("visibilitychange", () => { if (!document.hidden && convs[selected]) { convs[selected].unread = 0; renderAll(); } });

  window.Dash = { claim: claim, state: () => ({ agentId: agentId, queue: queue, selected: selected, convs: convs }) };

  // tu dang nhap neu co ?agent=ten hoac da dang nhap truoc do trong tab nay
  const qs = new URLSearchParams(location.search);
  const pre = qs.get("agent") || ss("lc_agent");
  if (pre) login(pre, qs.get("key") || ss("lc_key") || ""); else $("#login-name").focus();
})();