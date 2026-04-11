# HOLON MESH ORCHESTRATOR — AUDIT REPORT

**Audit ID:** `MESH-AUDIT-20260411-001`  
**Repository:** `szachmacik/holon-mesh-orchestrator`  
**Auditor:** Claude (distributed mesh audit — pilot run)  
**Date:** 2026-04-11  
**Commit audited:** `6154d73` (master)  

---

## EXECUTIVE SUMMARY

| Category | Status | Score |
|---|---|---|
| **Build Health** | WARN | 6/10 |
| **Security** | CRITICAL | 3/10 |
| **Code Quality** | WARN | 5/10 |
| **Dependencies** | OK | 7/10 |
| **Documentation** | FAIL | 1/10 |
| **Tests** | FAIL | 0/10 |
| **Docker/Infra** | WARN | 5/10 |
| **Overall** | **NEEDS ATTENTION** | **3.9/10** |

---

## CRITICAL ISSUES (must fix)

### SEC-001: Hardcoded Supabase URL
- **File:** `index.js:74`
- **Issue:** `SUPABASE_URL` has a hardcoded fallback URL pointing to production Supabase instance
- **Risk:** Credential/endpoint leak in public repo
- **Fix:** Remove fallback, require env var → **FIXED in this commit**

### SEC-002: CORS Wildcard in Production
- **File:** `index.js:76`
- **Issue:** `CORS_ORIGIN` defaults to `"*"` — any origin can call the API
- **Risk:** Cross-site request forgery, unauthorized API access
- **Fix:** Restrict to known domains → **FIXED in this commit**

### SEC-003: No Authentication on API Endpoints
- **File:** `index.js` (all routes)
- **Issue:** All REST and WebSocket endpoints are publicly accessible without any auth
- **Risk:** Anyone can register nodes, send signals, broadcast to entire mesh
- **Recommendation:** Add API key or JWT validation middleware

### SEC-004: No Input Validation on POST Endpoints
- **File:** `index.js` (`/api/mesh/signal`, `/api/mesh/broadcast`, `/kairos/add`)
- **Issue:** JSON.parse with minimal validation — no schema enforcement
- **Risk:** Injection, resource exhaustion via malformed payloads

---

## HIGH ISSUES

### BUILD-001: Dockerfile Missing kairos.js
- **File:** `Dockerfile`
- **Issue:** Only copies `package.json` and `index.js`, but NOT `kairos.js`
- **Impact:** kairos.js module unavailable in container (currently inlined in index.js, but standalone module is orphaned)
- **Fix:** → **FIXED in this commit**

### CODE-001: Module System Mismatch
- **File:** `kairos.js` vs `index.js`
- **Issue:** `kairos.js` uses ES modules (`import/export`), `index.js` uses CommonJS (`require`). No `"type": "module"` in package.json
- **Impact:** `kairos.js` cannot be imported by `index.js` — would throw SyntaxError
- **Status:** Not blocking (kairos logic is duplicated inline in index.js)

### CODE-002: Duplicated KAIR.OS Implementation
- **File:** `index.js` (lines 31-175) and `kairos.js` (entire file)
- **Issue:** Full KAIR.OS scheduler is implemented TWICE — once inline in index.js and once as standalone module
- **Impact:** Maintenance burden, divergence risk, 18KB wasted
- **Recommendation:** Refactor to single source of truth

### INFRA-001: State Persistence to /tmp
- **File:** `index.js:62`
- **Issue:** `KAIROS_PERSIST_PATH = '/tmp/kairos-state.json'` — /tmp is ephemeral in containers
- **Impact:** All KAIR.OS task state lost on container restart
- **Recommendation:** Mount volume or use Supabase for persistence

---

## MEDIUM ISSUES

### INFRA-002: No Rate Limiting
- All HTTP endpoints accept unlimited requests
- Risk of DoS via signal/broadcast flooding

### INFRA-003: Health Check Incomplete
- `/health` only checks process uptime, not Supabase connectivity
- No dependency health verification

### CODE-003: No .gitignore
- Missing .gitignore — risk of committing node_modules, .env, IDE files
- **Fix:** → **ADDED in this commit**

### CODE-004: No package-lock.json
- Not committed — dependency versions may drift between environments
- **Recommendation:** Generate and commit lock file

### DEPS-001: Dependency Versions
- `@supabase/supabase-js: ^2.39.0` — OK (latest major)
- `ws: ^8.16.0` — OK (latest major)
- No known CVEs at audit time

---

## LOW ISSUES

### DOC-001: No README
- No project documentation, setup instructions, or API reference
- **Recommendation:** Add minimal README with endpoints and setup

### TEST-001: Zero Test Coverage
- No test files, no test script in package.json
- **Recommendation:** Add health check and routing unit tests

### CODE-005: No .env.example
- Required environment variables undocumented
- **Recommendation:** Add .env.example listing SUPABASE_URL, SUPABASE_KEY, PORT, CORS_ORIGIN

---

## NETWORK TOPOLOGY ANALYSIS

```
TIER 0 (Core):      orchestrator (1 node)
TIER 1 (Archangels): metatron, michal, gabriel, raziel (4 nodes)
TIER 2 (Angels):     12 nodes (metatron→sandalfon)
TOTAL:               17 pre-registered nodes
```

**Routing patterns:** SHORTEST_PATH, BROADCAST, REDUNDANT, NEURAL_PROPAGATE  
**KAIR.OS tasks:** Auto-registers up to 12 mesh nodes for health monitoring  
**Biblical scheduling:** Sabbath(7), Desert(40), Pentecost(50), Jubilee(50), Forgiveness(490), Abundance(153)  

---

## FIXES APPLIED IN THIS AUDIT

1. **SEC-001 fixed:** Removed hardcoded Supabase URL fallback
2. **SEC-002 fixed:** Restricted CORS to ofshore.dev domains
3. **BUILD-001 fixed:** Dockerfile now copies all JS files
4. **CODE-003 fixed:** Added .gitignore
5. **Added:** .env.example documenting required env vars

---

## DELTA BASELINE

This is the first audit. All findings establish the **baseline** for future delta-based audits.

**Next audit will only check:**
- New commits since `6154d73`
- Changes to flagged files
- Resolution of CRITICAL/HIGH issues above

---

*Generated by Holon Mesh Distributed Audit System v1.0*  
*Pattern: pilot-run → teach network → parallel-propagate*
