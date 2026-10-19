/* Live Chat widget cho khach hang.
 * Nhung vao bat ky trang nao bang 1 the:  <script src="chat-widget.js"></script>
 * Tuy chon (dat TRUOC the script):  window.LIVECHAT_API = "http://127.0.0.1:8001"
 * Chong XSS: noi dung tin nhan luon dua vao DOM bang textContent, khong bao gio innerHTML. */
(function () {
  "use strict";
  if (window.__livechatLoaded) return;
  window.__livechatLoaded = true;

  var API = String(window.LIVECHAT_API || location.origin).replace(/\/$/, "");
  var WS_BASE = API.replace(/^http/, "ws");
  var PING_MS = 25000;                              // Redis presence het han sau 90s -> ping < 30s
  var FATAL = { 4400: 1, 4403: 1, 4404: 1, 4409: 1, 4410: 1 };   // ma dong server -> khong ket noi lai

  // ---------- danh tinh khach (moi tab 1 khach; ?customer=ten de gia lap nhieu khach) ----------
  function ss(k, v) {
    try { if (v === undefined) return sessionStorage.getItem(k); sessionStorage.setItem(k, v); } catch (e) { /* bo qua */ }
    return null;
  }
  var q = new URLSearchParams(location.search).get("customer");
  var customerId = (q || ss("lc_customer") || "kh_" + Math.random().toString(36).slice(2, 8)).slice(0, 64);
  ss("lc_customer", customerId);

  // ---------- trang thai ----------
  var S = {
    open: false, ws: null, conv: null, closed: false, historyLoaded: false, seen: {}, pending: [],
    nextBefore: null, unread: 0, retry: 0, agentOnline: false, server: null, bannerOn: false,
    pingTimer: null, queueTimer: null, reconnectTimer: null, queueText: ""
  };

  // ---------- tien ich ----------
  function el(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text != null) n.textContent = text;
    return n;
  }
  function api(path, opts) {
    return fetch(API + path, opts).then(function (r) {
      if (!r.ok) { var e = new Error("HTTP " + r.status); e.status = r.status; throw e; }
      return r.json();
    });
  }
  function hhmm(iso) {
    var d = new Date(iso);
    return isNaN(d) ? "" : d.toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit" });
  }

  // ---------- giao dien ----------
  var CSS = [
    ".lc-root{position:fixed;right:20px;bottom:20px;z-index:99999;font:14px/1.45 system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;color:#1f2937}",
    ".lc-root *{box-sizing:border-box}",
    ".lc-bubble{width:60px;height:60px;border-radius:50%;border:0;cursor:pointer;background:#c2410c;color:#fff;font-size:26px;box-shadow:0 6px 20px rgba(0,0,0,.25);position:relative;transition:transform .15s}",
    ".lc-bubble:hover{transform:scale(1.06)}",
    ".lc-badge{position:absolute;top:-4px;right:-4px;min-width:22px;height:22px;padding:0 6px;border-radius:11px;background:#dc2626;color:#fff;font-size:12px;font-weight:700;display:none;align-items:center;justify-content:center}",
    ".lc-panel{display:none;flex-direction:column;width:360px;height:520px;max-height:calc(100vh - 100px);background:#fff;border-radius:16px;box-shadow:0 12px 40px rgba(0,0,0,.28);overflow:hidden;margin-bottom:12px}",
    ".lc-root.lc-open .lc-panel{display:flex}",
    ".lc-head{background:#c2410c;color:#fff;padding:12px 14px;display:flex;align-items:center;gap:10px}",
    ".lc-head-t{flex:1;min-width:0}",
    ".lc-title{font-weight:700;font-size:15px}",
    ".lc-sub{font-size:12px;opacity:.92;display:flex;align-items:center;gap:6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}",
    ".lc-dot{width:8px;height:8px;border-radius:50%;background:#fbbf24;flex:none}",
    ".lc-dot.on{background:#4ade80}.lc-dot.off{background:#9ca3af}",
    ".lc-x{background:transparent;border:0;color:#fff;font-size:22px;cursor:pointer;line-height:1;padding:2px 6px}",
    ".lc-banner{display:none;background:#fef3c7;color:#92400e;padding:6px 12px;font-size:12px;text-align:center}",
    ".lc-body{flex:1;overflow-y:auto;padding:12px;background:#faf7f5;display:flex;flex-direction:column;gap:8px;scroll-behavior:smooth}",
    ".lc-msg{max-width:82%;padding:8px 12px;border-radius:14px;white-space:pre-wrap;word-wrap:break-word;overflow-wrap:anywhere}",
    ".lc-msg small{display:block;font-size:10.5px;opacity:.65;margin-top:3px}",
    ".lc-me{align-self:flex-end;background:#c2410c;color:#fff;border-bottom-right-radius:4px}",
    ".lc-them{align-self:flex-start;background:#fff;border:1px solid #e7e0da;border-bottom-left-radius:4px}",
    ".lc-sys{align-self:center;font-size:12px;color:#6b7280;background:#efe9e4;padding:4px 10px;border-radius:10px;text-align:center}",
    ".lc-sys.err{background:#fee2e2;color:#991b1b}",
    ".lc-more{align-self:center;border:1px solid #d6cdc6;background:#fff;color:#6b7280;border-radius:12px;padding:4px 12px;font-size:12px;cursor:pointer}",
    ".lc-foot{display:flex;gap:8px;padding:10px;border-top:1px solid #eee;background:#fff}",
    ".lc-in{flex:1;resize:none;border:1px solid #d6cdc6;border-radius:12px;padding:8px 10px;font:inherit;max-height:90px;outline:none}",
    ".lc-in:focus{border-color:#c2410c}.lc-in:disabled{background:#f3f4f6}",
    ".lc-send{border:0;background:#c2410c;color:#fff;border-radius:12px;padding:0 14px;cursor:pointer;font-weight:600}",
    ".lc-send:disabled{opacity:.45;cursor:not-allowed}",
    ".lc-note{font-size:10.5px;color:#9ca3af;text-align:center;padding:0 0 6px;background:#fff}",
    ".lc-start{position:absolute;inset:0;background:#faf7f5;display:none;flex-direction:column;align-items:center;justify-content:center;gap:10px;padding:24px;text-align:center}",
    ".lc-start h3{margin:0;font-size:18px}.lc-start p{margin:0;color:#6b7280}",
    ".lc-btn{border:0;background:#c2410c;color:#fff;border-radius:12px;padding:10px 18px;font-size:14px;font-weight:600;cursor:pointer}",
    ".lc-wrap{position:relative;flex:1;display:flex;flex-direction:column;min-height:0}",
    "@media (max-width:480px){.lc-root{right:10px;bottom:10px}.lc-panel{width:calc(100vw - 20px)}}"
  ].join("");

  var root = el("div", "lc-root");
  var style = el("style"); style.textContent = CSS; document.head.appendChild(style);

  var bubble = el("button", "lc-bubble", "💬"); bubble.setAttribute("aria-label", "Mo chat ho tro");
  var badge = el("span", "lc-badge"); bubble.appendChild(badge);

  var panel = el("div", "lc-panel");
  var head = el("div", "lc-head");
  var headT = el("div", "lc-head-t");
  headT.appendChild(el("div", "lc-title", "Hỗ trợ khách hàng"));
  var sub = el("div", "lc-sub"); var dot = el("span", "lc-dot"); var subText = el("span", null, "Chưa kết nối");
  sub.appendChild(dot); sub.appendChild(subText); headT.appendChild(sub);
  var closeBtn = el("button", "lc-x", "×"); closeBtn.setAttribute("aria-label", "Dong cua so chat");
  head.appendChild(headT); head.appendChild(closeBtn);

  var banner = el("div", "lc-banner");
  var wrap = el("div", "lc-wrap");
  var body = el("div", "lc-body");
  var start = el("div", "lc-start");
  var startTitle = el("h3", null, "Xin chào 👋");
  var startText = el("p", null, "Bạn cần hỗ trợ? Nhân viên của cửa hàng sẽ trả lời ngay khi có thể.");
  var startBtn = el("button", "lc-btn", "Bắt đầu trò chuyện");
  start.appendChild(startTitle); start.appendChild(startText); start.appendChild(startBtn);
  wrap.appendChild(body); wrap.appendChild(start);

  var foot = el("div", "lc-foot");
  var input = el("textarea", "lc-in"); input.rows = 1; input.maxLength = 2000; input.placeholder = "Nhập tin nhắn…"; input.disabled = true;
  var sendBtn = el("button", "lc-send", "Gửi"); sendBtn.disabled = true;
  foot.appendChild(input); foot.appendChild(sendBtn);
  var note = el("div", "lc-note", "Bạn: " + customerId);

  panel.appendChild(head); panel.appendChild(banner); panel.appendChild(wrap); panel.appendChild(foot); panel.appendChild(note);
  root.appendChild(panel); root.appendChild(bubble);
  document.body.appendChild(root);

  // ---------- hien thi ----------
  function nearBottom() { return body.scrollHeight - body.scrollTop - body.clientHeight < 80; }
  function toBottom() { body.scrollTop = body.scrollHeight; }
  function showBanner(text) { banner.textContent = text; banner.style.display = text ? "block" : "none"; S.bannerOn = !!text; }
  function sys(text, isErr) { var n = el("div", "lc-sys" + (isErr ? " err" : ""), text); body.appendChild(n); toBottom(); }
  function setInputEnabled(on) { input.disabled = !on; sendBtn.disabled = !on; if (on && S.open) input.focus(); }
  function setBadge(n) {
    S.unread = n; badge.textContent = n > 9 ? "9+" : String(n); badge.style.display = n > 0 ? "flex" : "none";
  }

  function bubbleOf(m) {
    var mine = m.sender_type === "customer";
    var n = el("div", "lc-msg " + (mine ? "lc-me" : "lc-them"), m.content);
    n.appendChild(el("small", null, hhmm(m.sent_at)));
    return n;
  }

  function updateStatus() {
    var c = S.conv, text, cls = "";
    if (S.closed) { text = "Cuộc trò chuyện đã kết thúc"; cls = "off"; }
    else if (!S.ws || S.ws.readyState !== 1) { text = "Đang kết nối…"; }
    else if (!c) { text = "Đang kết nối…"; }
    else if (c.status === "waiting") { text = S.queueText || "Đang chờ nhân viên…"; }
    else if (S.agentOnline) { text = "Nhân viên đang trực tuyến"; cls = "on"; }
    else { text = "Nhân viên tạm vắng - tin nhắn vẫn được lưu"; cls = "off"; }
    subText.textContent = text + (S.server ? " · server " + S.server : "");
    dot.className = "lc-dot " + cls;
  }

  // ---------- tin nhan ----------
  function renderMessage(m, live) {
    if (S.seen[m.id]) return;
    S.seen[m.id] = 1;
    var stick = nearBottom() || m.sender_type === "customer";
    body.appendChild(bubbleOf(m));
    if (stick) toBottom();
    if (live && m.sender_type !== "customer" && !S.open) setBadge(S.unread + 1);
  }

  function onMessage(m) {
    if (!S.historyLoaded) { S.pending.push(m); return; }   // dang tai lich su: giu lai, tranh chen sai thu tu
    renderMessage(m, true);
  }

  function loadHistory() {
    var first = Object.keys(S.seen).length === 0;
    return api("/api/conversations/" + S.conv.id + "/messages?limit=50").then(function (d) {
      var fresh = d.messages.filter(function (m) { return !S.seen[m.id]; }).reverse();   // cu -> moi
      fresh.forEach(function (m) { renderMessage(m, false); });
      if (first) { S.nextBefore = d.next_before; updateMore(); }
      S.historyLoaded = true;
      var p = S.pending; S.pending = [];
      p.forEach(function (m) { renderMessage(m, true); });
    });
  }

  var moreBtn = null;
  function updateMore() {
    if (moreBtn) { moreBtn.remove(); moreBtn = null; }
    if (!S.nextBefore) return;
    moreBtn = el("button", "lc-more", "Xem tin cũ hơn");
    moreBtn.onclick = loadOlder;
    body.insertBefore(moreBtn, body.firstChild);
  }
  function loadOlder() {
    var before = S.nextBefore; if (!before) return;
    moreBtn.disabled = true;
    api("/api/conversations/" + S.conv.id + "/messages?limit=50&before=" + before).then(function (d) {
      var oldH = body.scrollHeight, anchor = moreBtn.nextSibling;
      d.messages.slice().reverse().forEach(function (m) {
        if (S.seen[m.id]) return;
        S.seen[m.id] = 1;
        body.insertBefore(bubbleOf(m), anchor);
      });
      S.nextBefore = d.next_before; updateMore();
      body.scrollTop += body.scrollHeight - oldH;
    }).catch(function () { moreBtn.disabled = false; sys("Không tải được tin cũ", true); });
  }

  // ---------- ket noi ----------
  function connect() {
    clearTimeout(S.reconnectTimer);
    var ws = new WebSocket(WS_BASE + "/ws/" + S.conv.id + "?role=customer&user_id=" + encodeURIComponent(customerId));
    S.ws = ws; S.historyLoaded = false; S.pending = [];
    updateStatus();

    ws.onopen = function () {
      clearInterval(S.pingTimer);
      S.pingTimer = setInterval(function () {
        if (ws.readyState === 1) ws.send(JSON.stringify({ type: "ping" }));
      }, PING_MS);
    };

    ws.onmessage = function (ev) {
      var d; try { d = JSON.parse(ev.data); } catch (e) { return; }
      if (d.type === "joined") {
        S.conv = d.conversation; S.server = d.server_id; S.retry = 0; showBanner("");
        S.agentOnline = false;
        loadHistory().then(function () { setInputEnabled(true); })
          .catch(function () { sys("Không tải được lịch sử tin nhắn", true); S.historyLoaded = true; setInputEnabled(true); });
        if (S.conv.status === "waiting") startQueuePolling();
        else if (S.conv.agent_id) {
          api("/api/presence/agent/" + encodeURIComponent(S.conv.agent_id)).then(function (p) { S.agentOnline = p.online; updateStatus(); }).catch(function () {});
        }
        updateStatus();
      } else if (d.type === "message") {
        onMessage(d.message);
      } else if (d.type === "presence" && d.role === "agent") {
        S.agentOnline = d.event === "join";
        if (d.event === "join") {
          if (S.conv) S.conv.status = "active";
          stopQueuePolling(); S.queueText = "";
          sys("Nhân viên hỗ trợ đã tham gia");
        } else sys("Nhân viên đã rời đi (có thể quay lại)");
        updateStatus();
      } else if (d.type === "closed") {
        endConversation();
      } else if (d.type === "error") {
        sys(d.detail || "Có lỗi xảy ra", true);
      }
    };

    ws.onclose = function (ev) {
      clearInterval(S.pingTimer);
      if (S.ws !== ws) return;                      // ket noi cu, bo qua
      setInputEnabled(false);
      if (S.closed) return;
      if (ev.code === 4410) return endConversation();
      if (FATAL[ev.code]) {
        showBanner(ev.code === 4404 ? "Cuộc trò chuyện không còn tồn tại" : "Không thể vào cuộc trò chuyện (mã " + ev.code + ")");
        return showStart();
      }
      showBanner("Mất kết nối, đang thử lại…");
      updateStatus();
      var delay = Math.min(1000 * Math.pow(2, S.retry), 15000); S.retry++;
      S.reconnectTimer = setTimeout(connect, delay);
    };
  }

  function endConversation() {
    if (S.closed) return;
    S.closed = true; stopQueuePolling(); clearInterval(S.pingTimer);
    setInputEnabled(false); updateStatus();
    sys("Cuộc trò chuyện đã kết thúc. Cảm ơn bạn!");
    var again = el("button", "lc-btn", "Bắt đầu cuộc trò chuyện mới");
    again.onclick = function () { resetChat(); startNew(); };
    var box = el("div", "lc-sys"); box.appendChild(again); body.appendChild(box); toBottom();
  }

  // ---------- vi tri trong hang cho ----------
  function pollQueue() {
    api("/api/conversations/" + S.conv.id + "/queue-position").then(function (r) {
      if (!S.conv || S.conv.status !== "waiting") return;
      S.queueText = r.position ? "Đang chờ nhân viên · vị trí " + r.position + "/" + r.total : "Đang chờ nhân viên…";
      updateStatus();
    }).catch(function () {});
  }
  function startQueuePolling() { stopQueuePolling(); pollQueue(); S.queueTimer = setInterval(pollQueue, 8000); }
  function stopQueuePolling() { clearInterval(S.queueTimer); S.queueTimer = null; }

  // ---------- vong doi cuoc chat ----------
  function showStart() { start.style.display = "flex"; setInputEnabled(false); }
  function hideStart() { start.style.display = "none"; }

  function resetChat() {
    clearTimeout(S.reconnectTimer); stopQueuePolling(); clearInterval(S.pingTimer);
    if (S.ws) { var old = S.ws; S.ws = null; try { old.close(); } catch (e) {} }
    S.conv = null; S.closed = false; S.historyLoaded = false; S.seen = {}; S.pending = []; S.nextBefore = null;
    S.agentOnline = false; S.queueText = ""; S.server = null; S.retry = 0; moreBtn = null;
    body.textContent = ""; showBanner(""); updateStatus();
  }

  function startNew() {
    hideStart(); showBanner("");
    api("/api/conversations", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ customer_id: customerId })
    }).then(function (conv) { S.conv = conv; connect(); })
      .catch(function () { showBanner("Không kết nối được máy chủ. Vui lòng thử lại."); showStart(); });
  }

  function resumeOrStart() {
    showBanner("");
    api("/api/customers/" + encodeURIComponent(customerId) + "/conversations?limit=1").then(function (list) {
      var last = list[0];
      if (last && last.status !== "closed") { hideStart(); S.conv = { id: last.id, status: last.status }; connect(); }
      else showStart();
    }).catch(function () { showBanner("Không kết nối được máy chủ. Vui lòng thử lại."); showStart(); });
  }

  // ---------- su kien nguoi dung ----------
  var started = false;
  function setOpen(on) {
    S.open = on; root.classList.toggle("lc-open", on);
    if (on) {
      setBadge(0); toBottom();
      if (!started) { started = true; resumeOrStart(); }
      else if (!input.disabled) input.focus();
    }
  }
  function send() {
    var text = input.value.trim();
    if (!text || !S.ws || S.ws.readyState !== 1 || !S.historyLoaded) return;
    S.ws.send(JSON.stringify({ type: "message", content: text }));
    input.value = ""; input.style.height = "auto";
  }

  bubble.onclick = function () { setOpen(!S.open); };
  closeBtn.onclick = function () { setOpen(false); };
  startBtn.onclick = startNew;
  sendBtn.onclick = send;
  input.addEventListener("keydown", function (e) {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); }
  });
  input.addEventListener("input", function () {
    input.style.height = "auto"; input.style.height = Math.min(input.scrollHeight, 90) + "px";
  });

  window.LiveChat = { open: function () { setOpen(true); }, close: function () { setOpen(false); }, customerId: customerId };
})();