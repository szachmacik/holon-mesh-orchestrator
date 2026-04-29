// HOLON-META: {"morphic_field":"agent-state:4c67a2b1-6830-44ec-97b1-7c8f93722add"}
// HOLON-META: {
//   purpose: "HOLON orchestrator - agent coordination",
//   agents_notes: "KAPITAN: Strategic decisions | HEALER: Auto-recovery",
//   cost_impact: "Token optimization via Code Memory Protocol",
//   wiki: "32d6d069-74d6-8164-a6d5-f41c3d26ae9b"
// }

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
const { readFileSync, writeFileSync, existsSync } = require("fs");
const https = require("https");

// ── KAIR.OS Native Scheduler (zero external deps) ──────────────────────────────
const KAIROS_CONSTANTS = {
  TRINITY: 3, SABBATH: 7, APOSTLES: 12, DESERT: 40,
  PENTECOST: 50, JUBILEE: 50, FORGIVENESS: 490, ABUNDANCE: 153,
};

function createKairosTask(config) {
  return {
    name: config.name, url: config.url,
    method: config.method || 'GET', body: config.body || null,
    headers: config.headers || {}, interval: config.interval || 60,
    priority: config.priority || 5, tags: config.tags || [],
    minInterval: config.minInterval || 10, maxInterval: config.maxInterval || 7200,
    successes: 0, failures: 0, totalRuns: 0, cycle: 0,
    intervalMs: (config.interval || 60) * 1000, health: 1.0,
    avgMs: 0, consecFails: 0, lastMs: 0, lastResult: 'pending',
    lastRun: null, nextRun: null, desertDays: 0, forgiveness: 0,
    sabbath: false, promisedLand: false, abundance: false, pentecost: false,
    trinityPath: 1, jubileeCount: 0, pattern: 'learning',
    history: [], active: true, createdAt: Date.now(),
  };
}

function kairosNativeFetch(url, method = 'GET', body = null, headers = {}) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    const isHttps = url.startsWith('https');
    const lib = isHttps ? https : http;
    let parsed;
    try { parsed = new URL(url); } catch { return resolve({ ok: false, ms: 0 }); }
    const opts = {
      hostname: parsed.hostname, port: parsed.port || (isHttps ? 443 : 80),
      path: parsed.pathname + parsed.search, method,
      headers: { 'User-Agent': 'KAIR.OS/2.0', ...headers }, timeout: 8000,
    };
    const req = lib.request(opts, (res) => {
      res.on('data', () => {}); res.on('end', () => {
        resolve({ ok: res.statusCode < 400, ms: Date.now() - t0, status: res.statusCode });
      });
    });
    req.on('error', (e) => resolve({ ok: false, ms: Date.now() - t0, error: e.message }));
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, ms: 8000, error: 'timeout' }); });
    if (body) { const s = typeof body === 'string' ? body : JSON.stringify(body); req.write(s); }
    req.end();
  });
}

const kairosTasks = new Map();
const kairosTimers = new Map();
const kairosListeners = new Map();
const KAIROS_PERSIST_PATH = '/tmp/kairos-state.json';

function kairosPersist() {
  try {
    const data = {};
    for (const [name, state] of kairosTasks) data[name] = state;
    writeFileSync(KAIROS_PERSIST_PATH, JSON.stringify(data, null, 2));
  } catch {}
}

function kairosLoad() {
  try {
    if (existsSync(KAIROS_PERSIST_PATH)) {
      const data = JSON.parse(readFileSync(KAIROS_PERSIST_PATH, 'utf8'));
      for (const [name, state] of Object.entries(data)) {
        kairosTasks.set(name, state);
        if (state.active) kairosScheduleNext(name, state.intervalMs);
      }
      console.log(`[KAIR.OS] Loaded ${kairosTasks.size} tasks from persistence`);
    }
  } catch {}
}

function kairosScheduleNext(name, delayMs) {
  const existing = kairosTimers.get(name);
  if (existing) clearTimeout(existing);
  const jitter = Math.floor(delayMs * 0.05 * Math.random());
  const state = kairosTasks.get(name);
  if (state) state.nextRun = Date.now() + delayMs + jitter;
  const timer = setTimeout(() => kairosTick(name), delayMs + jitter);
  kairosTimers.set(name, timer);
}

function kairosBroadcast(name, state, ok, ms) {
  const msg = {
    event: 'tick', name, ok, ms,
    intervalS: Math.round(state.intervalMs / 1000), health: state.health,
    desert: state.desertDays, cycle: state.cycle, sabbath: state.sabbath,
    promised: state.promisedLand, abundance: state.abundance, pentecost: state.pentecost,
    pattern: state.pattern, trinity: state.trinityPath,
    successes: state.successes, failures: state.failures, jubilee: state.jubileeCount,
    lastResult: state.lastResult, timestamp: Date.now(),
  };
  const listeners = kairosListeners.get(name);
  if (listeners) for (const cb of listeners) { try { cb(msg); } catch {} }
  const all = kairosListeners.get('*');
  if (all) for (const cb of all) { try { cb(msg); } catch {} }
  // Also broadcast via WebSocket to mesh clients
  wss && wss.clients.forEach(client => {
    if (client.readyState === 1) {
      try { client.send(JSON.stringify({ type: 'KAIROS_TICK', ...msg })); } catch {}
    }
  });
}

async function kairosTick(name) {
  const state = kairosTasks.get(name);
  if (!state || !state.active) return;
  state.cycle++; state.totalRuns++; state.lastRun = Date.now();
  // Sabbath
  if (state.cycle % KAIROS_CONSTANTS.SABBATH === 0 && !state.sabbath) {
    state.sabbath = true;
    state.intervalMs = Math.round(state.intervalMs * 1.2);
    state.lastResult = 'sabbath';
    kairosTasks.set(name, state); kairosBroadcast(name, state, false, 0);
    kairosScheduleNext(name, state.intervalMs); kairosPersist();
    console.log(`[KAIR.OS] ${name} → SABBATH`);
    return;
  }
  if (state.sabbath) { state.sabbath = false; state.intervalMs = Math.max(state.minInterval * 1000, Math.round(state.intervalMs * 0.75)); }
  // Execute
  const t0 = Date.now();
  let result;
  try { result = await kairosNativeFetch(state.url, state.method, state.body, state.headers); }
  catch (e) { result = { ok: false, ms: Date.now() - t0, error: e.message }; }
  const ok = result.ok; const elapsed = result.ms || (Date.now() - t0);
  if (ok) {
    state.successes++; state.consecFails = 0; state.desertDays++;
    state.health = Math.min(1.0, state.health * 0.9 + 0.1);
    state.avgMs = state.avgMs * 0.8 + elapsed * 0.2; state.lastMs = elapsed; state.lastResult = 'ok';
    const base = state.interval * 1000;
    if (state.successes <= 5) state.pattern = 'learning';
    else if (state.intervalMs < base * 0.6) state.pattern = 'improving';
    else if (state.intervalMs > base * 1.3) state.pattern = 'degrading';
    else state.pattern = 'stable';
    if (state.successes % 5 === 0) state.intervalMs = Math.max(state.minInterval * 1000, Math.round(state.intervalMs * 0.85));
    if (state.health > 0.95 && elapsed < 500) state.intervalMs = Math.max(state.minInterval * 1000, Math.round(state.intervalMs * 0.95));
    if (state.desertDays >= KAIROS_CONSTANTS.DESERT && !state.promisedLand) { state.promisedLand = true; state.intervalMs = state.minInterval * 1000; console.log(`[KAIR.OS] ${name} → PROMISED LAND 🌟`); }
    if (state.totalRuns === KAIROS_CONSTANTS.PENTECOST && !state.pentecost) { state.pentecost = true; state.intervalMs = Math.max(state.minInterval * 1000, Math.round(state.intervalMs / 2)); console.log(`[KAIR.OS] ${name} → PENTECOST 🔥`); }
    if (state.successes >= KAIROS_CONSTANTS.ABUNDANCE && !state.abundance) { state.abundance = true; console.log(`[KAIR.OS] ${name} → ABUNDANCE 🐟`); }
  } else {
    state.failures++; state.consecFails++; state.desertDays = 0; state.forgiveness++;
    state.health = Math.max(0.0, state.health * 0.7); state.lastResult = result.error || 'fail';
    state.lastMs = elapsed; state.pattern = 'degrading'; state.promisedLand = false;
    const mult = Math.min(8.0, Math.pow(1.5, state.consecFails));
    state.intervalMs = Math.min(state.maxInterval * 1000, Math.round(state.intervalMs * mult));
    if (state.forgiveness >= KAIROS_CONSTANTS.FORGIVENESS) {
      state.active = false; kairosTasks.set(name, state); kairosBroadcast(name, state, false, elapsed);
      kairosPersist(); console.log(`[KAIR.OS] ${name} → BEYOND FORGIVENESS`); return;
    }
  }
  state.trinityPath = (state.trinityPath % KAIROS_CONSTANTS.TRINITY) + 1;
  if (state.cycle % KAIROS_CONSTANTS.JUBILEE === 0) {
    state.intervalMs = state.interval * 1000; state.consecFails = 0; state.forgiveness = 0;
    state.jubileeCount++; console.log(`[KAIR.OS] ${name} → JUBILEE #${state.jubileeCount} 🎺`);
  }
  state.history.push({ t: Date.now(), ok, ms: elapsed, int: Math.round(state.intervalMs / 1000), h: state.health, result: state.lastResult });
  if (state.history.length > 20) state.history = state.history.slice(-20);
  kairosTasks.set(name, state); kairosBroadcast(name, state, ok, elapsed);
  kairosScheduleNext(name, state.intervalMs); kairosPersist();
  console.log(`[KAIR.OS] ${name} → ${ok ? '✓' : '✗'} ${elapsed}ms | h=${state.health.toFixed(2)} | next=${Math.round(state.intervalMs/1000)}s | ${state.pattern}`);
}

function kairosAdd(config) {
  if (kairosTasks.has(config.name)) { kairosStop(config.name); }
  const state = createKairosTask(config);
  kairosTasks.set(config.name, state);
  kairosScheduleNext(config.name, state.intervalMs);
  kairosPersist();
  return { ok: true, name: config.name, intervalMs: state.intervalMs };
}

function kairosStop(name) {
  const timer = kairosTimers.get(name);
  if (timer) { clearTimeout(timer); kairosTimers.delete(name); }
  const state = kairosTasks.get(name);
  if (state) { state.active = false; kairosPersist(); }
  return { stopped: true, name };
}

function kairosSystemInfo() {
  const all = Array.from(kairosTasks.values());
  return {
    system: 'KAIR.OS', version: '2.0.0-node',
    runtime: 'Node.js native (zero extra dependencies)',
    dependencies: 'ZERO additional — uses existing http/https/fs modules',
    biblicalPrinciples: {
      zasada_3: 'Trinity path rotation', zasada_7: 'Sabbath rest every 7th cycle',
      zasada_12: '12 max batch', zasada_40: 'Promised Land after 40 successes',
      zasada_50_pentecost: 'Pentecost: interval halved at run 50',
      zasada_50_jubilee: 'Jubilee: reset every 50 cycles',
      zasada_70x7: '490 failure tolerance', zasada_153: 'Abundance at 153 successes',
    },
    stats: {
      totalTasks: kairosTasks.size,
      activeTasks: all.filter(t => t.active).length,
      totalRuns: all.reduce((s, t) => s + t.totalRuns, 0),
      totalSuccesses: all.reduce((s, t) => s + t.successes, 0),
      totalFailures: all.reduce((s, t) => s + t.failures, 0),
      promisedLandCount: all.filter(t => t.promisedLand).length,
      abundanceCount: all.filter(t => t.abundance).length,
    },
  };
}

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

// ── KAIR.OS: Register all mesh nodes as scheduled health tasks ────────────────
kairosLoad();
setTimeout(() => {
  if (kairosTasks.size === 0) {
    const meshTasks = Array.from(meshNodes.values())
      .filter(n => n.domain)
      .map(n => ({
        name: n.id,
        url: `https://${n.domain}`,
        method: 'GET',
        interval: n.tier === 0 ? 30 : n.tier === 1 ? 60 : 120,
        priority: n.tier === 0 ? 9 : n.tier === 1 ? 7 : 5,
        tags: [`tier-${n.tier}`, n.id.includes('arch') ? 'archangel' : 'angel'],
        minInterval: 10, maxInterval: 3600,
      }));
    const batch = meshTasks.slice(0, KAIROS_CONSTANTS.APOSTLES);
    batch.forEach(t => kairosAdd(t));
    console.log(`[KAIR.OS] Auto-registered ${batch.length} mesh nodes as scheduled tasks`);
  }
}, 2000);

// ── KAIR.OS HTTP Endpoints ──────────────────────────────────────────────────────
const kairosRoutes = {
  // GET /kairos — system info
  'GET /kairos': (req, res) => {
    res.writeHead(200); res.end(JSON.stringify(kairosSystemInfo(), null, 2));
  },
  // GET /kairos/tasks — list all tasks
  'GET /kairos/tasks': (req, res) => {
    const all = Array.from(kairosTasks.values()).map(t => ({
      name: t.name, url: t.url, active: t.active, health: t.health,
      pattern: t.pattern, lastResult: t.lastResult,
      intervalS: Math.round(t.intervalMs / 1000),
      successes: t.successes, failures: t.failures, totalRuns: t.totalRuns,
      milestones: { promisedLand: t.promisedLand, pentecost: t.pentecost, abundance: t.abundance, sabbath: t.sabbath, jubileeCount: t.jubileeCount },
      nextRunIn: t.nextRun ? Math.max(0, t.nextRun - Date.now()) : null,
    }));
    res.writeHead(200); res.end(JSON.stringify(all, null, 2));
  },
};

// Inject KAIR.OS routes into existing HTTP server
const _originalHandler = server.listeners('request')[0];
server.removeAllListeners('request');
server.on('request', (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', CORS_ORIGIN);
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Content-Type', 'application/json');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
  const url = new URL(req.url, `http://localhost:${PORT}`);
  const routeKey = `${req.method} ${url.pathname}`;
  // KAIR.OS routes
  if (url.pathname === '/kairos' && req.method === 'GET') { return kairosRoutes['GET /kairos'](req, res); }
  if (url.pathname === '/kairos/tasks' && req.method === 'GET') { return kairosRoutes['GET /kairos/tasks'](req, res); }
  if (url.pathname.startsWith('/kairos/status/') && req.method === 'GET') {
    const name = url.pathname.slice(15);
    const state = kairosTasks.get(name);
    if (!state) { res.writeHead(404); res.end(JSON.stringify({ error: 'not found' })); return; }
    res.writeHead(200); res.end(JSON.stringify({ ...state, history: undefined, nextRunIn: state.nextRun ? Math.max(0, state.nextRun - Date.now()) : null }, null, 2)); return;
  }
  if (url.pathname.startsWith('/kairos/history/') && req.method === 'GET') {
    const name = url.pathname.slice(16);
    const state = kairosTasks.get(name);
    res.writeHead(200); res.end(JSON.stringify(state ? state.history : [])); return;
  }
  if (url.pathname.startsWith('/kairos/stop/') && req.method === 'POST') {
    const name = url.pathname.slice(13);
    res.writeHead(200); res.end(JSON.stringify(kairosStop(name))); return;
  }
  if (url.pathname === '/kairos/add' && req.method === 'POST') {
    let body = ''; req.on('data', d => body += d);
    req.on('end', () => {
      try { const config = JSON.parse(body); res.writeHead(200); res.end(JSON.stringify(kairosAdd(config))); }
      catch { res.writeHead(400); res.end(JSON.stringify({ error: 'invalid json' })); }
    }); return;
  }
  if (url.pathname === '/kairos/bulk-add' && req.method === 'POST') {
    let body = ''; req.on('data', d => body += d);
    req.on('end', () => {
      try {
        const { tasks } = JSON.parse(body);
        const batch = (tasks || []).slice(0, KAIROS_CONSTANTS.APOSTLES);
        const results = batch.map(t => kairosAdd(t));
        res.writeHead(200); res.end(JSON.stringify({ added: results.length, total: (tasks || []).length, results }));
      } catch { res.writeHead(400); res.end(JSON.stringify({ error: 'invalid json' })); }
    }); return;
  }
  // Fall through to original handler
  _originalHandler(req, res);
});

// ── Start ──────────────────────────────────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`[HOLON MESH ORCHESTRATOR] Running on port ${PORT}`);
  console.log(`[HOLON MESH ORCHESTRATOR] ${meshNodes.size} nodes registered`);
  console.log(`[HOLON MESH ORCHESTRATOR] WebSocket: ws://localhost:${PORT}/ws`);
});
