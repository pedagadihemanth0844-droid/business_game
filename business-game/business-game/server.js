// Tiny game server: serves the page and relays each room's game state over WebSocket.
const http = require("http"), fs = require("fs"), path = require("path"), os = require("os");
const { WebSocketServer } = require("ws");
const PORT = process.env.PORT || 3000;
const rooms = new Map();   // "games/ABCDE" -> latest game state
const subs = new Map();    // "games/ABCDE" -> Set of sockets
const hist = new Map();    // "games/ABCDE" -> last chat messages

const server = http.createServer((req, res) => {
  const f = fs.existsSync(path.join(__dirname, "public", "index.html")) ? path.join(__dirname, "public", "index.html") : path.join(__dirname, "index.html");
  fs.readFile(f, (err, data) => {
    if (err) { res.writeHead(500); return res.end("public/index.html is missing"); }
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(data);
  });
});

const wss = new WebSocketServer({ server, maxPayload: 200 * 1024 });
wss.on("error", () => {}); // port errors are handled in start()

function broadcast(p, d) {
  const msg = JSON.stringify({ t: "snap", p, d });
  (subs.get(p) || []).forEach(w => w.readyState === 1 && w.send(msg));
}

wss.on("connection", ws => {
  ws.on("message", raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    const p = String(m.p || "").slice(0, 40);
    if (m.t === "get") {
      ws.send(JSON.stringify({ t: "got", k: m.k, d: rooms.get(p) || null }));
    } else if (m.t === "sub") {
      if (!subs.has(p)) subs.set(p, new Set());
      subs.get(p).add(ws);
      const d = rooms.get(p);
      if (d) ws.send(JSON.stringify({ t: "snap", p, d }));
      (hist.get(p) || []).forEach(c => ws.send(JSON.stringify(c)));
    } else if (m.t === "chat") {
      if (!(subs.get(p) && subs.get(p).has(ws))) return;
      const c = { t: "chat", p, i: String(m.i || "").slice(0, 40), n: String(m.n || "?").slice(0, 12), c: String(m.c || "#fff").slice(0, 9), x: String(m.x || "").slice(0, 140) };
      if (!c.x) return;
      const h = hist.get(p) || []; h.push(c); if (h.length > 50) h.shift(); hist.set(p, h);
      const msg = JSON.stringify(c); subs.get(p).forEach(w => w.readyState === 1 && w.send(msg));
    } else if (m.t === "set" && m.d && typeof m.d === "object") {
      const cur = rooms.get(p);
      if (!cur || m.d.v > cur.v) { rooms.set(p, m.d); broadcast(p, m.d); }   // newer version wins
      else ws.send(JSON.stringify({ t: "snap", p, d: cur }));                // stale write: resync sender
    }
  });
  ws.on("close", () => subs.forEach(s => s.delete(ws)));
});

// keep connections alive on hosts that close idle sockets
setInterval(() => wss.clients.forEach(w => w.readyState === 1 && w.ping()), 25000);

function start(port) {
  server.once("error", e => {
    if (e.code === "EADDRINUSE") { console.log("Port " + port + " is busy, trying " + (port + 1) + "..."); start(port + 1); }
    else throw e;
  });
  server.listen(port, "0.0.0.0", () => {
    console.log("\nBusiness Rajahmundry server running!");
    console.log("  This computer :  http://localhost:" + port);
    Object.values(os.networkInterfaces()).flat().filter(i => i.family === "IPv4" && !i.internal)
      .forEach(i => console.log("  Friends on same Wi-Fi :  http://" + i.address + ":" + port));
  });
}
start(Number(PORT));
