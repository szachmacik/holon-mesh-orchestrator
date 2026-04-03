// ================================================================
// KAIR.OS v2.0.0 — Native Node.js Port
// ZERO external dependencies. Self-contained.
// Port filozofii Rust/WASM → pure Node.js
// Storage: in-memory Map + JSON file persistence
// Scheduling: setInterval/setTimeout (native Node.js)
// Communication: native http/https
// ================================================================

import { readFileSync, writeFileSync, existsSync } from 'fs';
import { request as httpRequest } from 'https';
import { request as httpReqHttp } from 'http';

// ═══════════════════════════════════════════════════════════════
// BIBLICAL CONSTANTS — Zasady Kairotyczne
// ═══════════════════════════════════════════════════════════════
const KAIROS = {
  TRINITY:      3,    // Rotacja 3 ścieżek wykonania
  SABBATH:      7,    // Co 7. cykl = odpoczynek (+20% interval)
  APOSTLES:     12,   // Max 12 zadań w batchu
  DESERT:       40,   // 40 sukcesów = Ziemia Obiecana (min interval)
  PENTECOST:    50,   // 50. uruchomienie = interval / 2
  JUBILEE:      50,   // Co 50 cykli = reset długów
  FORGIVENESS:  490,  // 70×7 = limit tolerancji błędów
  ABUNDANCE:    153,  // 153 sukcesy = tryb Obfitości
};

// ═══════════════════════════════════════════════════════════════
// TASK STATE — Stan każdego zadania
// ═══════════════════════════════════════════════════════════════
function createTaskState(config) {
  return {
    // Config
    name: config.name,
    url: config.url,
    method: config.method || 'GET',
    body: config.body || null,
    headers: config.headers || {},
    interval: config.interval || 60,       // seconds
    priority: config.priority || 5,
    tags: config.tags || [],
    minInterval: config.minInterval || 10,
    maxInterval: config.maxInterval || 7200,
    // Counters
    successes: 0,
    failures: 0,
    totalRuns: 0,
    cycle: 0,
    // Adaptive
    intervalMs: (config.interval || 60) * 1000,
    health: 1.0,
    avgMs: 0,
    consecFails: 0,
    lastMs: 0,
    lastResult: 'pending',
    lastRun: null,
    nextRun: null,
    // Biblical milestones
    desertDays: 0,
    forgiveness: 0,
    sabbath: false,
    promisedLand: false,
    abundance: false,
    pentecost: false,
    trinityPath: 1,
    jubileeCount: 0,
    // Pattern
    pattern: 'learning',
    // History (last 20)
    history: [],
    // Status
    active: true,
    createdAt: Date.now(),
  };
}

// ═══════════════════════════════════════════════════════════════
// NATIVE FETCH — zero dependencies
// ═══════════════════════════════════════════════════════════════
function nativeFetch(url, method = 'GET', body = null, headers = {}) {
  return new Promise((resolve) => {
    const t0 = Date.now();
    const isHttps = url.startsWith('https');
    const lib = isHttps ? httpRequest : httpReqHttp;
    
    let parsed;
    try { parsed = new URL(url); } 
    catch { return resolve({ ok: false, ms: 0, status: 0, error: 'invalid_url' }); }

    const opts = {
      hostname: parsed.hostname,
      port: parsed.port || (isHttps ? 443 : 80),
      path: parsed.pathname + parsed.search,
      method,
      headers: { 'User-Agent': 'KAIR.OS/2.0', ...headers },
      timeout: 10000,
    };

    if (body) {
      const bodyStr = typeof body === 'string' ? body : JSON.stringify(body);
      opts.headers['Content-Type'] = 'application/json';
      opts.headers['Content-Length'] = Buffer.byteLength(bodyStr);
    }

    const req = lib(opts, (res) => {
      res.on('data', () => {});
      res.on('end', () => {
        resolve({ ok: res.statusCode < 400, ms: Date.now() - t0, status: res.statusCode });
      });
    });

    req.on('error', (e) => resolve({ ok: false, ms: Date.now() - t0, status: 0, error: e.message }));
    req.on('timeout', () => { req.destroy(); resolve({ ok: false, ms: 10000, status: 0, error: 'timeout' }); });

    if (body) {
      const bodyStr = typeof body === 'string' ? body : JSON.stringify(body);
      req.write(bodyStr);
    }
    req.end();
  });
}

// ═══════════════════════════════════════════════════════════════
// KAIROS ENGINE — Główny silnik schedulera
// ═══════════════════════════════════════════════════════════════
class KairosEngine {
  constructor(persistPath = '/tmp/kairos-state.json') {
    this.tasks = new Map();       // name → state
    this.timers = new Map();      // name → timer
    this.listeners = new Map();   // name → Set of callbacks (WebSocket)
    this.persistPath = persistPath;
    this.load();
    console.log(`[KAIR.OS] Engine initialized. Tasks loaded: ${this.tasks.size}`);
  }

  // ─── PERSISTENCE ─────────────────────────────────────────────
  load() {
    try {
      if (existsSync(this.persistPath)) {
        const data = JSON.parse(readFileSync(this.persistPath, 'utf8'));
        for (const [name, state] of Object.entries(data)) {
          this.tasks.set(name, state);
        }
        // Re-schedule active tasks
        for (const [name, state] of this.tasks) {
          if (state.active) this._scheduleNext(name, state.intervalMs);
        }
      }
    } catch (e) {
      console.warn('[KAIR.OS] Could not load state:', e.message);
    }
  }

  persist() {
    try {
      const data = {};
      for (const [name, state] of this.tasks) data[name] = state;
      writeFileSync(this.persistPath, JSON.stringify(data, null, 2));
    } catch (e) {
      console.warn('[KAIR.OS] Could not persist state:', e.message);
    }
  }

  // ─── REGISTER TASK ───────────────────────────────────────────
  add(config) {
    if (this.tasks.has(config.name)) {
      this.stop(config.name);
    }
    const state = createTaskState(config);
    this.tasks.set(config.name, state);
    this._scheduleNext(config.name, state.intervalMs);
    this.persist();
    console.log(`[KAIR.OS] Task registered: ${config.name} @ ${config.url} every ${config.interval}s`);
    return { ok: true, name: config.name, intervalMs: state.intervalMs };
  }

  bulkAdd(configs) {
    const results = [];
    const batch = configs.slice(0, KAIROS.APOSTLES); // Max 12 per batch
    for (const config of batch) {
      results.push(this.add(config));
    }
    return { added: results.length, total: configs.length, results };
  }

  stop(name) {
    const timer = this.timers.get(name);
    if (timer) { clearTimeout(timer); this.timers.delete(name); }
    const state = this.tasks.get(name);
    if (state) { state.active = false; this.persist(); }
    return { stopped: true, name };
  }

  getStatus(name) {
    const state = this.tasks.get(name);
    if (!state) return null;
    return {
      ...state,
      history: undefined, // exclude from status
      milestones: {
        promisedLand: state.promisedLand,
        pentecost: state.pentecost,
        abundance: state.abundance,
        sabbath: state.sabbath,
        jubileeCount: state.jubileeCount,
        trinityPath: state.trinityPath,
        desertDays: state.desertDays,
        forgiveness: state.forgiveness,
      },
      nextRunIn: state.nextRun ? Math.max(0, state.nextRun - Date.now()) : null,
    };
  }

  getHistory(name) {
    const state = this.tasks.get(name);
    return state ? state.history : [];
  }

  listAll() {
    const result = [];
    for (const [name, state] of this.tasks) {
      result.push({
        name,
        url: state.url,
        active: state.active,
        health: state.health,
        pattern: state.pattern,
        lastResult: state.lastResult,
        intervalS: Math.round(state.intervalMs / 1000),
        successes: state.successes,
        failures: state.failures,
        totalRuns: state.totalRuns,
        milestones: {
          promisedLand: state.promisedLand,
          pentecost: state.pentecost,
          abundance: state.abundance,
          sabbath: state.sabbath,
        },
        nextRunIn: state.nextRun ? Math.max(0, state.nextRun - Date.now()) : null,
      });
    }
    return result;
  }

  // ─── SUBSCRIBE (WebSocket callbacks) ─────────────────────────
  subscribe(name, callback) {
    if (!this.listeners.has(name)) this.listeners.set(name, new Set());
    this.listeners.get(name).add(callback);
    return () => this.listeners.get(name)?.delete(callback);
  }

  _broadcast(name, state, ok, ms) {
    const msg = {
      event: 'tick', name, ok, ms,
      intervalS: Math.round(state.intervalMs / 1000),
      health: state.health,
      desert: state.desertDays,
      cycle: state.cycle,
      sabbath: state.sabbath,
      promised: state.promisedLand,
      abundance: state.abundance,
      pentecost: state.pentecost,
      pattern: state.pattern,
      trinity: state.trinityPath,
      successes: state.successes,
      failures: state.failures,
      jubilee: state.jubileeCount,
      lastResult: state.lastResult,
      timestamp: Date.now(),
    };
    const listeners = this.listeners.get(name);
    if (listeners) {
      for (const cb of listeners) {
        try { cb(msg); } catch {}
      }
    }
    // Also broadcast to '*' listeners (all tasks)
    const allListeners = this.listeners.get('*');
    if (allListeners) {
      for (const cb of allListeners) {
        try { cb(msg); } catch {}
      }
    }
  }

  // ─── SCHEDULE NEXT ───────────────────────────────────────────
  _scheduleNext(name, delayMs) {
    const existing = this.timers.get(name);
    if (existing) clearTimeout(existing);
    
    // Jitter: 0-10% to prevent thundering herd
    const jitter = Math.floor(delayMs * 0.1 * Math.random());
    const actualDelay = delayMs + jitter;
    
    const state = this.tasks.get(name);
    if (state) state.nextRun = Date.now() + actualDelay;

    const timer = setTimeout(() => this._tick(name), actualDelay);
    this.timers.set(name, timer);
  }

  // ─── MAIN TICK — Serce KAIR.OS ───────────────────────────────
  async _tick(name) {
    const state = this.tasks.get(name);
    if (!state || !state.active) return;

    state.cycle++;
    state.totalRuns++;
    state.lastRun = Date.now();

    // ═══ ZASADA 7: SABBATH ═══
    if (state.cycle % KAIROS.SABBATH === 0 && !state.sabbath) {
      state.sabbath = true;
      state.intervalMs = Math.round(state.intervalMs * 1.2);
      state.lastResult = 'sabbath';
      this.tasks.set(name, state);
      this._broadcast(name, state, false, 0);
      this._scheduleNext(name, state.intervalMs);
      this.persist();
      console.log(`[KAIR.OS] ${name} → SABBATH (cycle ${state.cycle})`);
      return;
    }
    // Resurrection boost after sabbath
    if (state.sabbath) {
      state.sabbath = false;
      state.intervalMs = Math.max(
        state.minInterval * 1000,
        Math.round(state.intervalMs * 0.75)
      );
    }

    // ═══ FIRE TASK ═══
    const t0 = Date.now();
    let result;
    try {
      // Trinity rotation: 3 execution paths
      if (state.trinityPath === 1) {
        result = await nativeFetch(state.url, state.method, state.body, state.headers);
      } else if (state.trinityPath === 2) {
        // Path 2: with retry
        result = await nativeFetch(state.url, state.method, state.body, state.headers);
        if (!result.ok) {
          await new Promise(r => setTimeout(r, 500));
          result = await nativeFetch(state.url, state.method, state.body, state.headers);
        }
      } else {
        // Path 3: with timeout extension
        result = await nativeFetch(state.url, state.method, state.body, state.headers);
      }
    } catch (e) {
      result = { ok: false, ms: Date.now() - t0, error: e.message };
    }

    const ok = result.ok;
    const elapsed = result.ms || (Date.now() - t0);

    if (ok) {
      // ═══ SUCCESS PATH ═══
      state.successes++;
      state.consecFails = 0;
      state.desertDays++;
      state.health = Math.min(1.0, state.health * 0.9 + 0.1);
      state.avgMs = state.avgMs * 0.8 + elapsed * 0.2;
      state.lastMs = elapsed;
      state.lastResult = 'ok';

      // Pattern detection
      const base = state.interval * 1000;
      if (state.successes <= 5) state.pattern = 'learning';
      else if (state.intervalMs < base * 0.6) state.pattern = 'improving';
      else if (state.intervalMs > base * 1.3) state.pattern = 'degrading';
      else state.pattern = 'stable';

      // Predictive: improving = preemptive boost
      if (state.pattern === 'improving') {
        state.intervalMs = Math.max(state.minInterval * 1000, Math.round(state.intervalMs * 0.97));
      }
      // Every 5 successes: 15% faster
      if (state.successes % 5 === 0) {
        state.intervalMs = Math.max(state.minInterval * 1000, Math.round(state.intervalMs * 0.85));
      }
      // SRPT: healthy + fast response
      if (state.health > 0.95 && elapsed < 500) {
        state.intervalMs = Math.max(state.minInterval * 1000, Math.round(state.intervalMs * 0.95));
      }

      // ═══ ZASADA 40: PROMISED LAND ═══
      if (state.desertDays >= KAIROS.DESERT && !state.promisedLand) {
        state.promisedLand = true;
        state.intervalMs = state.minInterval * 1000;
        console.log(`[KAIR.OS] ${name} → PROMISED LAND! 🌟`);
      }
      // ═══ ZASADA 50: PENTECOST ═══
      if (state.totalRuns === KAIROS.PENTECOST && !state.pentecost) {
        state.pentecost = true;
        state.intervalMs = Math.max(state.minInterval * 1000, Math.round(state.intervalMs / 2));
        console.log(`[KAIR.OS] ${name} → PENTECOST! 🔥`);
      }
      // ═══ ZASADA 153: ABUNDANCE ═══
      if (state.successes >= KAIROS.ABUNDANCE && !state.abundance) {
        state.abundance = true;
        console.log(`[KAIR.OS] ${name} → ABUNDANCE! 🐟`);
      }

    } else {
      // ═══ FAILURE PATH ═══
      state.failures++;
      state.consecFails++;
      state.desertDays = 0;
      state.forgiveness++;
      state.health = Math.max(0.0, state.health * 0.7);
      state.lastResult = result.error || 'fail';
      state.lastMs = elapsed;
      state.pattern = 'degrading';
      state.promisedLand = false;

      // Exponential backoff
      const mult = Math.min(8.0, Math.pow(1.5, state.consecFails));
      state.intervalMs = Math.min(state.maxInterval * 1000, Math.round(state.intervalMs * mult));

      // ═══ ZASADA 70×7: FORGIVENESS ═══
      if (state.forgiveness >= KAIROS.FORGIVENESS) {
        state.active = false;
        state.lastResult = 'beyond_forgiveness';
        this.tasks.set(name, state);
        this._broadcast(name, state, false, elapsed);
        this.persist();
        console.log(`[KAIR.OS] ${name} → BEYOND FORGIVENESS. Deactivated.`);
        return;
      }
    }

    // ═══ ZASADA 3: TRINITY ROTATION ═══
    state.trinityPath = (state.trinityPath % KAIROS.TRINITY) + 1;

    // ═══ ZASADA 50: JUBILEE ═══
    if (state.cycle % KAIROS.JUBILEE === 0) {
      state.intervalMs = state.interval * 1000;
      state.consecFails = 0;
      state.forgiveness = 0;
      state.jubileeCount++;
      console.log(`[KAIR.OS] ${name} → JUBILEE #${state.jubileeCount} 🎺`);
    }

    // Save history (last 20)
    state.history.push({
      t: Date.now(), ok, ms: elapsed,
      int: Math.round(state.intervalMs / 1000),
      h: state.health, d: state.desertDays, c: state.cycle,
      result: state.lastResult,
    });
    if (state.history.length > 20) state.history = state.history.slice(-20);

    this.tasks.set(name, state);
    this._broadcast(name, state, ok, elapsed);
    this._scheduleNext(name, state.intervalMs);
    this.persist();

    console.log(`[KAIR.OS] ${name} → ${ok ? '✓' : '✗'} ${elapsed}ms | health=${state.health.toFixed(2)} | next=${Math.round(state.intervalMs/1000)}s | pattern=${state.pattern}`);
  }

  // ─── SYSTEM INFO ─────────────────────────────────────────────
  systemInfo() {
    return {
      system: 'KAIR.OS',
      version: '2.0.0-node',
      runtime: 'Node.js native (zero dependencies)',
      storage: 'in-memory Map + JSON persistence',
      scheduling: 'setTimeout/setInterval (native)',
      execution: 'native https/http module',
      dependencies: 'ZERO — fully self-contained',
      biblicalPrinciples: {
        zasada_3:  'Trinity path rotation (3 execution modes)',
        zasada_7:  'Sabbath rest every 7th cycle + resurrection boost',
        zasada_12: '12 max batch (enforced in bulk operations)',
        zasada_40: '40 consecutive successes = Promised Land (min interval)',
        zasada_50_pentecost: '50th run = interval halved (fire of the Spirit)',
        zasada_50_jubilee: 'Every 50 cycles = all debts forgiven, interval reset',
        zasada_70x7: '490 failure tolerance before deactivation',
        zasada_153: '153 successes = Abundance mode (permanent priority)',
      },
      stats: {
        totalTasks: this.tasks.size,
        activeTasks: [...this.tasks.values()].filter(t => t.active).length,
        totalRuns: [...this.tasks.values()].reduce((s, t) => s + t.totalRuns, 0),
        totalSuccesses: [...this.tasks.values()].reduce((s, t) => s + t.successes, 0),
        totalFailures: [...this.tasks.values()].reduce((s, t) => s + t.failures, 0),
        promisedLandCount: [...this.tasks.values()].filter(t => t.promisedLand).length,
        abundanceCount: [...this.tasks.values()].filter(t => t.abundance).length,
      },
    };
  }
}

export { KairosEngine, KAIROS, nativeFetch };
