/**
 * Holon Neural Mesh Orchestrator v1.0
 * =====================================
 * Central routing hub for the Holon Mesh network.
 * 
 * Features:
 * - WebSocket server for real-time node communication
 * - Smart routing: SHORTEST_PATH, BROADCAST, REDUNDANT, NEURAL_PROPAGATE
 * - Health-aware failover (dead nodes bypassed automatically)
 * - Signal propagation engine with TTL
 * - REST API for dashboard integration
 * - Supabase persistence for mesh state
 * 
 * Endpoints:
 *   GET  /health              — orchestrator health
 *   GET  /api/mesh/nodes      — all registered nodes + status
 *   GET  /api/mesh/routes     — active routing table
 *   GET  /api/mesh/signals    — recent signal log
 *   POST /api/mesh/signal     — send signal through mesh
 *   POST /api/mesh/broadcast  — broadcast to all nodes
 *   WS   /ws                  — WebSocket for node registration & real-time
 */

const http = require("http");
const { WebSocketServer } = require("ws");
const { createClient } = require("@supabase/supabase-js");

// ── Config ─────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
const SUPABASE_URL = process.env.SUPABASE_URL || "https://zqlfxakzqkzxoqhzpgqh.supabase.co";
const SUPABASE_KEY = process.env.SUPABASE_KEY || "";
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";

const supabase = SUPABASE_KEY ? createClient(SUPABASE_URL, SUPABASE_KEY) : null;

// ── In-memory mesh state ───────────────────────────────────────────────────────
const meshNodes = new Map(); // nodeId -> { id, name, tier, status, domain, color, lastSeen, ws, latency }
const signalLog = [];        // last 100 signals
const MAX_LOG = 100;

// Pre-register known nodes (will be updated by heartbeats)
const KNOWN_NODES = [
  { id: "orchestrator",   name: "HOLON MESH",  tier: 0, domain: "mesh.ofshore.dev",     color: "#b44fff" },
  { id: "metatron-arch",  name: "METATRON",    tier: 1, domain: "metatron.ofshore.dev",  color: "#ffd700" },
  { id: "michal-arch",    name: "MICHAŁ",      tier: 1, domain: "michal.ofshore.dev",    color: "#ff8c00" },
  { id: "gabriel-arch",   name: "GABRIEL",     tier: 1, domain: "gabriel.ofshore.dev",   color: "#b44fff" },
  { id: "raziel-arch",    name: "RAZIEL",      tier: 1, domain: "raziel.ofshore.dev",    color: "#ff3366" },
  { id: "angel-metatron", name: "METATRON",    tier: 2, domain: "metatron.ofshore.dev",  color: "#00d4ff" },
  { id: "angel-michal",   name: "MICHAŁ",      tier: 2, domain: "michal.ofshore.dev",    color: "#00d4ff" },
  { id: "angel-gabriel",  name: "GABRIEL",     tier: 2, domain: "gabriel.ofshore.dev",   color: "#00d4ff" },
  { id: "angel-raziel",   name: "RAZIEL",      tier: 2, domain: "raziel.ofshore.dev",    color: "#00d4ff" },
  { id: "angel-uriel",    name: "URIEL",       tier: 2, domain: "uriel.ofshore.dev",     color: "#00d4ff" },
  { id: "angel-sariel",   name: "SARIEL",      tier: 2, domain: "sariel.ofshore.dev",    color: "#00d4ff" },
  { id: "angel-jophiel",  name: "JOPHIEL",     tier: 2, domain: "jophiel.ofshore.dev",   color: "#00d4ff" },
  { id: "angel-chamuel",  name: "CHAMUEL",     tier: 2, domain: "chamuel.ofshore.dev",   color: "#00d4ff" },
  { id: "angel-haniel",   name: "HANIEL",      tier: 2, domain: "haniel.ofshore.dev",    color: "#00d4ff" },
  { id: "angel-zadkiel",  name: "ZADKIEL",     tier: 2, domain: "zadkiel.ofshore.dev",   color: "#00d4ff" },
  { id: "angel-ariel",    name: "ARIEL",       tier: 2, domain: "ariel.ofshore.dev",     color: "#00d4ff" },
  { id: "angel-sandalfon",name: "SANDALFON",   tier: 2, domain: "sandalfon.ofshore.dev", color: "#00d4ff" },
];

KNOWN_NODES.forEach(n => {
  meshNodes.set(n.id, { ...n, status: "unknown", lastSeen: null, ws: null, latency: null });
});

// ── Routing table ──────────────────────────────────────────────────────────────
function buildRoutingTable() {
  const nodes = Array.from(meshNodes.values());
  const routes = [];

  // Orchestrator ↔ Archangels
  nodes.filter(n => n.tier === 1).forEach(arch => {
    routes.push({ source: "orchestrator", target: arch.id, weight: 0.9, active: arch.status !== "offline" });
  });

  // Archangel ↔ Archangel (full mesh)
  const archangels = nodes.filter(n => n.tier === 1);
  for (let i = 0; i < archangels.length; i++) {
    for (let j = i + 1; j < archangels.length; j++) {
      routes.push({
        source: archangels[i].id, target: archangels[j].id,
        weight: 0.6,
        active: archangels[i].status !== "offline" && archangels[j].status !== "offline"
      });
    }
  }

  // Angels → Archangels (round-robin + redundancy)
  const angels = nodes.filter(n => n.tier === 2);
  angels.forEach((angel, idx) => {
    const arch = archangels[idx % archangels.length];
    const arch2 = archangels[(idx + 1) % archangels.length];
    if (arch) routes.push({ source: arch.id, target: angel.id, weight: 0.4, active: angel.status !== "offline" });
    if (arch2) routes.push({ source: arch2.id, target: angel.id, weight: 0.2, active: angel.status !== "offline" && arch2.status !== "offline" });
  });

  return routes;
}

// ── Signal routing engine ──────────────────────────────────────────────────────
function routeSignal(signal) {
  const { sourceId, targets, routing, payload, ttl = 10 } = signal;
  const nodes = Array.from(meshNodes.values());
  const activeNodes = nodes.filter(n => n.status !== "offline" && n.ws);

  let targetNodes = [];

  switch (routing) {
    case "BROADCAST":
      targetNodes = activeNodes.filter(n => n.id !== sourceId);
      break;
    case "SHORTEST_PATH":
      // Simple: direct to target if active, else route through nearest archangel
      targetNodes = (targets || []).map(tid => {
        const node = meshNodes.get(tid);
        if (node && node.status !== "offline") return node;
        // Fallback: find archangel with lowest latency
        const archangels = nodes.filter(n => n.tier === 1 && n.status !== "offline");
        return archangels.sort((a, b) => (a.latency || 999) - (b.latency || 999))[0];
      }).filter(Boolean);
      break;
    case "REDUNDANT":
      // Send through multiple paths
      targetNodes = (targets || []).flatMap(tid => {
        const direct = meshNodes.get(tid);
        const archangels = nodes.filter(n => n.tier === 1 && n.status !== "offline");
        return [direct, ...archangels.slice(0, 2)].filter(Boolean);
      });
      break;
    case "NEURAL_PROPAGATE":
      // Propagate like neural impulse: source → neighbors → their neighbors (up to TTL)
      const visited = new Set([sourceId]);
      let frontier = [sourceId];
      for (let hop = 0; hop < Math.min(ttl, 3); hop++) {
        const next = [];
        frontier.forEach(fid => {
          nodes.filter(n => !visited.has(n.id) && n.status !== "offline").forEach(n => {
            visited.add(n.id);
            next.push(n);
          });
        });
        targetNodes.push(...next.slice(0, 5)); // limit spread
        frontier = next.map(n => n.id);
        if (frontier.length === 0) break;
      }
      break;
    default:
      targetNodes = activeNodes.filter(n => n.id !== sourceId);
  }

  // Deliver signal to target nodes via WebSocket
  const delivered = [];
  targetNodes.forEach(node => {
    if (node.ws && node.ws.readyState === 1) {
      try {
        node.ws.send(JSON.stringify({ type: "SIGNAL", signal: { ...signal, deliveredTo: node.id } }));
        delivered.push(node.id);
      } catch (e) {}
    }
  });

  // Log signal
  const logEntry = {
    id: signal.id || `sig-${Date.now()}`,
    timestamp: new Date().toISOString(),
    sourceId,
    routing,
    targets: delivered,
    type: signal.type || "TASK",
    delivered: delivered.length,
  };
  signalLog.unshift(logEntry);
  if (signalLog.length > MAX_LOG) signalLog.pop();

  // Persist to Supabase
  if (supabase) {
    supabase.from("mesh_signals").insert(logEntry).then(() => {}).catch(() => {});
  }

  return { delivered, logEntry };
}

// ── HTTP Server ────────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  res.setHeader("Access-Control-Allow-Origin", CORS_ORIGIN);
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Content-Type", "application/json");

  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }

  const url = new URL(req.url, `http://localhost:${PORT}`);

  // Health check
  if (url.pathname === "/health") {
    res.writeHead(200);
    res.end(JSON.stringify({
      status: "ok",
      uptime: process.uptime(),
      nodes: meshNodes.size,
      activeNodes: Array.from(meshNodes.values()).filter(n => n.status !== "offline").length,
      timestamp: new Date().toISOString(),
    }));
    return;
  }

  // Mesh nodes
  if (url.pathname === "/api/mesh/nodes" && req.method === "GET") {
    const nodes = Array.from(meshNodes.values()).map(n => ({
      id: n.id, name: n.name, tier: n.tier, status: n.status,
      domain: n.domain, color: n.color, latency: n.latency,
      lastSeen: n.lastSeen, connected: !!n.ws,
    }));
    res.writeHead(200);
    res.end(JSON.stringify(nodes));
    return;
  }

  // Routing table
  if (url.pathname === "/api/mesh/routes" && req.method === "GET") {
    res.writeHead(200);
    res.end(JSON.stringify(buildRoutingTable()));
    return;
  }

  // Signal log
  if (url.pathname === "/api/mesh/signals" && req.method === "GET") {
    res.writeHead(200);
    res.end(JSON.stringify(signalLog.slice(0, 50)));
    return;
  }

  // Send signal
  if (url.pathname === "/api/mesh/signal" && req.method === "POST") {
    let body = "";
    req.on("data", d => body += d);
    req.on("end", () => {
      try {
        const signal = JSON.parse(body);
        const result = routeSignal(signal);
        res.writeHead(200);
        res.end(JSON.stringify({ ok: true, ...result }));
      } catch (e) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: "Invalid signal payload" }));
      }
    });
    return;
  }

  // Broadcast
  if (url.pathname === "/api/mesh/broadcast" && req.method === "POST") {
    let body = "";
    req.on("data", d => body += d);
    req.on("end", () => {
      try {
        const { sourceId = "orchestrator", payload, type = "BROADCAST" } = JSON.parse(body);
        const result = routeSignal({ sourceId, routing: "BROADCAST", type, payload, id: `bc-${Date.now()}` });
        res.writeHead(200);
        res.end(JSON.stringify({ ok: true, ...result }));
      } catch (e) {
        res.writeHead(400);
        res.end(JSON.stringify({ error: "Invalid broadcast payload" }));
      }
    });
    return;
  }

  res.writeHead(404);
  res.end(JSON.stringify({ error: "Not found" }));
});

// ── WebSocket Server ───────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server, path: "/ws" });

wss.on("connection", (ws, req) => {
  let nodeId = null;

  ws.on("message", (data) => {
    try {
      const msg = JSON.parse(data.toString());

      switch (msg.type) {
        case "REGISTER": {
          nodeId = msg.nodeId;
          const existing = meshNodes.get(nodeId) || {};
          meshNodes.set(nodeId, {
            ...existing,
            id: nodeId,
            name: msg.name || existing.name || nodeId,
            tier: msg.tier ?? existing.tier ?? 2,
            domain: msg.domain || existing.domain || "",
            color: msg.color || existing.color || "#00d4ff",
            status: "healthy",
            lastSeen: new Date().toISOString(),
            ws,
            latency: msg.latency || null,
          });
          console.log(`[MESH] Node registered: ${nodeId}`);
          ws.send(JSON.stringify({ type: "REGISTERED", nodeId, routes: buildRoutingTable() }));
          broadcastMeshUpdate();
          break;
        }

        case "HEARTBEAT": {
          if (nodeId && meshNodes.has(nodeId)) {
            const node = meshNodes.get(nodeId);
            node.status = msg.status || "healthy";
            node.lastSeen = new Date().toISOString();
            node.latency = msg.latency || node.latency;
            meshNodes.set(nodeId, node);
          }
          ws.send(JSON.stringify({ type: "HEARTBEAT_ACK", timestamp: Date.now() }));
          break;
        }

        case "SIGNAL": {
          // Node is forwarding a signal through the mesh
          const result = routeSignal({ ...msg.signal, sourceId: nodeId || msg.signal?.sourceId });
          ws.send(JSON.stringify({ type: "SIGNAL_ROUTED", ...result }));
          break;
        }
      }
    } catch (e) {
      console.error("[MESH] WS parse error:", e.message);
    }
  });

  ws.on("close", () => {
    if (nodeId && meshNodes.has(nodeId)) {
      const node = meshNodes.get(nodeId);
      node.status = "offline";
      node.ws = null;
      meshNodes.set(nodeId, node);
      console.log(`[MESH] Node disconnected: ${nodeId}`);
      broadcastMeshUpdate();
    }
  });
});

function broadcastMeshUpdate() {
  const nodes = Array.from(meshNodes.values()).map(n => ({
    id: n.id, name: n.name, tier: n.tier, status: n.status,
    domain: n.domain, color: n.color, latency: n.latency, lastSeen: n.lastSeen,
  }));
  wss.clients.forEach(client => {
    if (client.readyState === 1) {
      client.send(JSON.stringify({ type: "MESH_UPDATE", nodes, routes: buildRoutingTable() }));
    }
  });
}

// ── Auto-probe known nodes ─────────────────────────────────────────────────────
async function probeNodes() {
  const nodes = Array.from(meshNodes.values()).filter(n => n.domain && !n.ws);
  for (const node of nodes) {
    try {
      const start = Date.now();
      const ctrl = new AbortController();
      const timeout = setTimeout(() => ctrl.abort(), 3000);
      const res = await fetch(`https://${node.domain}`, { method: "HEAD", signal: ctrl.signal });
      clearTimeout(timeout);
      const latency = Date.now() - start;
      node.status = res.ok ? "healthy" : "degraded";
      node.latency = latency;
      node.lastSeen = new Date().toISOString();
    } catch {
      node.status = "offline";
      node.latency = null;
    }
    meshNodes.set(node.id, node);
  }
}

// Probe every 60 seconds
setInterval(probeNodes, 60000);
probeNodes(); // Initial probe

// ── Start ──────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`[HOLON MESH ORCHESTRATOR] Running on port ${PORT}`);
  console.log(`[HOLON MESH ORCHESTRATOR] ${meshNodes.size} nodes registered`);
  console.log(`[HOLON MESH ORCHESTRATOR] WebSocket: ws://localhost:${PORT}/ws`);
});
