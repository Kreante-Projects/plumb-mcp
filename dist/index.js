#!/usr/bin/env node
import {
  resolveAsset,
  searchAssets
} from "./chunk-J4ZHAR5B.js";

// src/index.ts
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";

// src/bridge/server.ts
import { createServer } from "http";
import { createWriteStream } from "fs";
import { basename, join as join3 } from "path";
import { tmpdir } from "os";
import { WebSocketServer } from "ws";

// src/bridge/protocol.ts
var BRIDGE_PORTS = [
  31337,
  31338,
  31339,
  31340,
  31341,
  31342,
  31343,
  31344,
  31345,
  31346
];

// src/bridge/store.ts
var BridgeStore = class {
  /** The port the bridge bound, or null if the bridge is not running. */
  port = null;
  /** Whether a plugin has completed the one-time pairing. */
  paired = false;
  pluginVersion = null;
  /** The latest selection streamed by the paired plugin. */
  selection = null;
  /** The file's screen inventory (all pages → top-level frames). */
  inventory = null;
  /** Last time any message arrived from the plugin (epoch ms). */
  lastSeen = 0;
  /**
   * Monotonic counter bumped on every inventory push (plugin debounces those
   * on `documentchange`, so the bump tracks "the file has changed somehow").
   * Cached node responses store the fileVersion they were generated at —
   * a mismatch on lookup means the cache is stale.
   */
  fileVersion = 0;
  /**
   * Server-side cache of `requestNode` responses, keyed `${nodeId}:${depth}`.
   * Hit rate matters most during drill-down loops and verify cycles where
   * the agent re-asks for parent/sibling subtrees that haven't changed.
   * Cleared wholesale on every fileVersion bump — simple, correct.
   */
  nodeCache = /* @__PURE__ */ new Map();
  /** Clear pairing-scoped state — called when the plugin disconnects. */
  reset() {
    this.paired = false;
    this.pluginVersion = null;
    this.selection = null;
    this.inventory = null;
    this.fileVersion = 0;
    this.nodeCache.clear();
  }
};
var bridge = new BridgeStore();

// src/errors.ts
var PlumbError = class extends Error {
  nextAction;
  constructor(message, nextAction) {
    super(message);
    this.name = "PlumbError";
    this.nextAction = nextAction;
  }
};
function toErrorPayload(e) {
  if (e instanceof PlumbError) {
    return { error: e.message, nextAction: e.nextAction };
  }
  return {
    error: e instanceof Error ? e.message : String(e),
    nextAction: "Unexpected failure \u2014 retry; if it persists, open an issue with this message."
  };
}

// src/meta.ts
import { readFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
function loadPackageMeta() {
  try {
    const here = dirname(fileURLToPath(import.meta.url));
    const pkgPath = join(here, "..", "package.json");
    return JSON.parse(readFileSync(pkgPath, "utf8"));
  } catch {
    return {};
  }
}
var pkg = loadPackageMeta();
var SERVER_NAME = pkg.name ?? "plumb-mcp";
var SERVER_VERSION = pkg.version ?? "0.0.0";

// src/studio/events.ts
import { EventEmitter } from "events";
var bus = new EventEmitter();
bus.setMaxListeners(64);
var RING_MAX = 200;
var ring = [];
function emitStudio(event) {
  const ev = { t: event.t ?? Date.now(), ...event };
  ring.push(ev);
  if (ring.length > RING_MAX) ring.shift();
  bus.emit("event", ev);
}
function onStudio(fn) {
  bus.on("event", fn);
  return () => bus.off("event", fn);
}
function recentStudio() {
  return ring.slice();
}

// src/studio/serve.ts
import { createReadStream, existsSync, statSync } from "fs";
import { extname, join as join2, normalize, sep } from "path";
import { fileURLToPath as fileURLToPath2 } from "url";
var STUDIO_DIR = fileURLToPath2(new URL("./studio", import.meta.url));
var CONTENT_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".png": "image/png",
  ".ico": "image/x-icon",
  ".woff2": "font/woff2",
  ".map": "application/json"
};
function studioAvailable() {
  return existsSync(join2(STUDIO_DIR, "index.html"));
}
function serveStudio(req, res) {
  if (!studioAvailable()) {
    res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    res.end(
      "Plumb Studio assets aren't built here. Use the published `plumb-mcp`, or run `npm run build` from source."
    );
    return;
  }
  let urlPath = decodeURIComponent((req.url ?? "/").split("?")[0] ?? "/");
  if (urlPath === "/" || urlPath === "") urlPath = "/index.html";
  let filePath = normalize(join2(STUDIO_DIR, urlPath));
  if (filePath !== STUDIO_DIR && !filePath.startsWith(STUDIO_DIR + sep)) {
    res.writeHead(403);
    res.end();
    return;
  }
  if (!existsSync(filePath) || !statSync(filePath).isFile()) {
    filePath = join2(STUDIO_DIR, "index.html");
  }
  const type = CONTENT_TYPES[extname(filePath)] ?? "application/octet-stream";
  res.writeHead(200, {
    "Content-Type": type,
    "Cache-Control": extname(filePath) === ".html" ? "no-store" : "public, max-age=3600"
  });
  createReadStream(filePath).pipe(res);
}

// src/bridge/server.ts
var pairedSocket = null;
var reqCounter = 0;
var uploadMap = /* @__PURE__ */ new Map();
var pending = /* @__PURE__ */ new Map();
var progressListeners = /* @__PURE__ */ new Map();
var inbound = /* @__PURE__ */ new Map();
var INBOUND_TTL_MS = 10 * 60 * 1e3;
var inboundCounter = 0;
var EXT_CONTENT_TYPE = {
  png: "image/png",
  jpg: "image/jpeg",
  jpeg: "image/jpeg",
  webp: "image/webp",
  gif: "image/gif",
  svg: "image/svg+xml"
};
function send(ws, msg) {
  try {
    ws.send(JSON.stringify(msg));
  } catch {
  }
}
function log(line) {
  process.stderr.write(`plumb bridge: ${line}
`);
}
function resolvePending(reqId, value) {
  const p = pending.get(reqId);
  if (!p) {
    log(`late reply for ${reqId} (already timed out or unknown)`);
    return;
  }
  clearTimeout(p.timer);
  pending.delete(reqId);
  p.resolve(value);
}
function rejectAllPending(error) {
  for (const p of pending.values()) {
    clearTimeout(p.timer);
    p.reject(error);
  }
  pending.clear();
  progressListeners.clear();
}
function request(build, timeoutMs, label) {
  return new Promise((resolve, reject) => {
    if (!pairedSocket) {
      reject(
        new PlumbError(
          "No Figma plugin is paired.",
          "Open your file in Figma, run the Plumb plugin, and click 'Pair with Plumb'."
        )
      );
      return;
    }
    const reqId = `r${++reqCounter}`;
    const timer = setTimeout(() => {
      pending.delete(reqId);
      progressListeners.delete(reqId);
      reject(
        new PlumbError(
          `The Plumb plugin did not answer the ${label} request in time.`,
          "Make sure the plugin is still running and paired in Figma, then retry."
        )
      );
    }, timeoutMs);
    pending.set(reqId, {
      resolve,
      reject,
      timer,
      timeoutMs,
      label
    });
    send(pairedSocket, build(reqId));
  });
}
async function requestNode(nodeId2, depth) {
  const key = `${nodeId2}:${depth ?? "all"}`;
  const cached = bridge.nodeCache.get(key);
  if (cached && cached.fileVersion === bridge.fileVersion) {
    return { doc: cached.doc, nodeName: cached.nodeName };
  }
  const result = await request(
    (reqId) => ({ t: "get-node", reqId, nodeId: nodeId2, depth }),
    6e4,
    "node"
  );
  bridge.nodeCache.set(key, { ...result, fileVersion: bridge.fileVersion });
  return result;
}
var assetRequestStart = /* @__PURE__ */ new Map();
function requestAssets(opts) {
  return request(
    (reqId) => {
      assetRequestStart.set(reqId, Date.now());
      return {
        t: "get-assets",
        reqId,
        nodeId: opts.nodeId ?? "",
        ids: opts.ids,
        list: opts.list,
        raw: opts.raw
      };
    },
    18e4,
    "assets"
  );
}
function requestScreenshot(nodeId2, scale, format) {
  return request(
    (reqId) => ({ t: "get-screenshot", reqId, nodeId: nodeId2, scale, format }),
    24e4,
    "screenshot"
  );
}
function requestSearch(query, type) {
  return request(
    (reqId) => ({ t: "get-search", reqId, query, type }),
    3e4,
    "search"
  );
}
function requestComponents() {
  return request(
    (reqId) => ({ t: "get-components", reqId }),
    6e4,
    "components"
  );
}
function requestApplyDesign(plan, onProgress) {
  return new Promise((resolve, reject) => {
    request(
      (reqId) => {
        if (onProgress) progressListeners.set(reqId, onProgress);
        return { t: "apply-design", reqId, plan };
      },
      6e5,
      "apply-design"
    ).then(resolve, reject);
  });
}
async function requestApplyFoundations(plan, dryRun) {
  return request(
    (reqId) => ({ t: "apply-foundations", reqId, plan, dryRun }),
    12e4,
    "foundations"
  );
}
function stageInboundAsset(bytes, ext) {
  const key = `a${++inboundCounter}${Date.now().toString(36)}`;
  const buf = Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
  const e = ext.toLowerCase().replace(/^\./, "");
  inbound.set(key, {
    bytes: buf,
    contentType: EXT_CONTENT_TYPE[e] ?? "application/octet-stream",
    expires: Date.now() + INBOUND_TTL_MS
  });
  return key;
}
function handleHttp(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }
  if (req.method === "GET") {
    const am = req.url && /^\/asset\/([A-Za-z0-9_-]+)\.([a-z0-9]+)$/.exec(req.url);
    if (am) {
      const key = am[1];
      const staged = inbound.get(key);
      if (!staged || staged.expires < Date.now()) {
        if (staged) inbound.delete(key);
        res.writeHead(404);
        res.end();
        return;
      }
      res.writeHead(200, {
        "Content-Type": staged.contentType,
        "Content-Length": staged.bytes.length
      });
      res.end(staged.bytes);
      return;
    }
    serveStudio(req, res);
    return;
  }
  const m = req.url && /^\/upload\/([A-Za-z0-9_-]+)\.([a-z0-9]+)$/.exec(req.url);
  if (!m || req.method !== "POST") {
    res.writeHead(404);
    res.end();
    return;
  }
  const reqId = m[1];
  const ext = m[2];
  const path = join3(tmpdir(), `plumb-${reqId}.${ext}`);
  const out = createWriteStream(path);
  req.pipe(out);
  out.on("finish", () => {
    uploadMap.set(reqId, { path });
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true }));
  });
  out.on("error", (err) => {
    log(`upload write failed: ${err.message}`);
    res.writeHead(500);
    res.end();
  });
}
function handleStudioClient(ws) {
  const sendEvent = (payload) => {
    if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(payload));
  };
  sendEvent({ t: "studio-hello", serverVersion: SERVER_VERSION, backlog: recentStudio() });
  const unsubscribe = onStudio((e) => sendEvent({ t: "studio-event", event: e }));
  ws.on("close", unsubscribe);
  ws.on("error", unsubscribe);
}
function bindPort(port) {
  return new Promise((resolve) => {
    const http = createServer(handleHttp);
    const wss = new WebSocketServer({ server: http });
    wss.on("error", () => void 0);
    http.once("listening", () => resolve({ wss, http }));
    http.once("error", () => resolve(null));
    http.listen(port, "127.0.0.1");
  });
}
async function startBridge() {
  let wss = null;
  let chosen = 0;
  for (const port of BRIDGE_PORTS) {
    const bound = await bindPort(port);
    if (bound) {
      wss = bound.wss;
      chosen = port;
      break;
    }
  }
  if (!wss) {
    log("no free port in range \u2014 plugin path disabled, REST path still works");
    return;
  }
  bridge.port = chosen;
  const sessionLabel = process.env.PLUMB_SESSION_NAME?.trim() || basename(process.cwd());
  log(`listening on 127.0.0.1:${chosen} as "${sessionLabel}"`);
  if (studioAvailable()) {
    log(`Plumb Studio (live cockpit): http://127.0.0.1:${chosen}/  \xB7  run \`plumb-mcp studio\` to open it`);
  }
  wss.on("connection", (ws, req) => {
    if (req.url && req.url.startsWith("/studio")) {
      handleStudioClient(ws);
      return;
    }
    log(`connection from origin ${req.headers.origin ?? "(none)"}`);
    send(ws, { t: "plumb-hello", serverVersion: SERVER_VERSION, sessionLabel });
    let thisPaired = false;
    ws.on("message", (raw) => {
      let msg;
      try {
        msg = JSON.parse(String(raw));
      } catch {
        return;
      }
      if (msg.t === "pair") {
        if (bridge.paired && !thisPaired) {
          send(ws, { t: "pair-rejected", reason: "Another Plumb plugin is already paired." });
          return;
        }
        bridge.paired = true;
        bridge.pluginVersion = msg.pluginVersion;
        bridge.lastSeen = Date.now();
        thisPaired = true;
        pairedSocket = ws;
        send(ws, { t: "paired" });
        log("plugin paired");
        return;
      }
      if (!thisPaired) return;
      bridge.lastSeen = Date.now();
      switch (msg.t) {
        case "selection":
          bridge.selection = msg.doc ? {
            doc: msg.doc,
            fileName: msg.fileName,
            pageName: msg.pageName,
            nodeName: msg.nodeName ?? "",
            receivedAt: Date.now()
          } : null;
          break;
        case "inventory":
          bridge.inventory = { fileName: msg.fileName, pages: msg.pages };
          bridge.fileVersion += 1;
          if (bridge.nodeCache.size > 0) bridge.nodeCache.clear();
          log(`inventory: ${msg.pages.reduce((n, p) => n + p.frames.length, 0)} screen(s)`);
          break;
        case "node":
          resolvePending(msg.reqId, { doc: msg.doc, nodeName: msg.nodeName });
          break;
        case "assets": {
          const filled = msg.assets.map((a, i) => {
            if (a.path !== null) return a;
            const key = `${msg.reqId}-${i}`;
            const upload = uploadMap.get(key);
            uploadMap.delete(key);
            return { ...a, path: upload?.path ?? null };
          });
          const startedAt = assetRequestStart.get(msg.reqId);
          assetRequestStart.delete(msg.reqId);
          if (startedAt !== void 0) {
            log(`assets reply: ${filled.length} asset(s) in ${Date.now() - startedAt}ms`);
          }
          resolvePending(msg.reqId, { assets: filled, error: msg.error });
          break;
        }
        case "screenshot": {
          const upload = uploadMap.get(msg.reqId);
          uploadMap.delete(msg.reqId);
          resolvePending(msg.reqId, {
            path: upload?.path ?? null,
            format: msg.format,
            nodeName: msg.nodeName,
            error: msg.error
          });
          break;
        }
        case "search":
          resolvePending(msg.reqId, { matches: msg.matches, error: msg.error });
          break;
        case "components":
          resolvePending(msg.reqId, {
            components: msg.components,
            instances: msg.instances,
            error: msg.error
          });
          break;
        case "apply-progress": {
          const p = pending.get(msg.reqId);
          if (p) {
            clearTimeout(p.timer);
            p.timer = setTimeout(() => {
              pending.delete(msg.reqId);
              progressListeners.delete(msg.reqId);
              p.reject(
                new PlumbError(
                  `The Plumb plugin did not answer the ${p.label} request in time.`,
                  "Make sure the plugin is still running and paired in Figma, then retry."
                )
              );
            }, p.timeoutMs);
          }
          progressListeners.get(msg.reqId)?.(msg);
          break;
        }
        case "applied":
          bridge.fileVersion += 1;
          if (bridge.nodeCache.size > 0) bridge.nodeCache.clear();
          progressListeners.delete(msg.reqId);
          resolvePending(msg.reqId, { result: msg.result, error: msg.error });
          break;
        case "foundations":
          resolvePending(
            msg.reqId,
            msg.result ?? {
              collections: [],
              textStyleIds: {},
              effectStyleIds: {},
              gridStyleIds: {},
              warnings: [msg.error ?? "foundations failed"]
            }
          );
          break;
        case "motion":
          resolvePending(
            msg.reqId,
            msg.result ?? { wired: 0, misses: [], error: msg.error }
          );
          break;
        case "pong":
          break;
      }
    });
    ws.on("close", () => {
      if (thisPaired) {
        pairedSocket = null;
        bridge.reset();
        rejectAllPending(
          new PlumbError(
            "The Plumb plugin disconnected mid-request.",
            "Re-run the Plumb plugin in Figma and pair again, then retry."
          )
        );
        log("plugin disconnected");
      }
    });
    ws.on("error", () => {
    });
  });
}

// src/env.ts
import { readFileSync as readFileSync2 } from "fs";
import { dirname as dirname2, join as join4 } from "path";
import { fileURLToPath as fileURLToPath3 } from "url";
function applyDotEnvFile(path) {
  let text;
  try {
    text = readFileSync2(path, "utf8");
  } catch {
    return;
  }
  for (const rawLine of text.split("\n")) {
    const trimmed = rawLine.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eq = trimmed.indexOf("=");
    if (eq === -1) continue;
    const key = trimmed.slice(0, eq).trim();
    let value = trimmed.slice(eq + 1).trim();
    if (value.startsWith('"') && value.endsWith('"') || value.startsWith("'") && value.endsWith("'")) {
      value = value.slice(1, -1);
    }
    if (key && process.env[key] === void 0) process.env[key] = value;
  }
}
function loadEnv() {
  applyDotEnvFile(join4(process.cwd(), ".env"));
  try {
    const here = dirname2(fileURLToPath3(import.meta.url));
    applyDotEnvFile(join4(here, "..", ".env"));
  } catch {
  }
}

// src/cli/fit.ts
import { existsSync as existsSync3, writeFileSync } from "fs";
import { resolve as resolvePath } from "path";

// src/cli/chrome.ts
import { existsSync as existsSync2 } from "fs";
import { execSync } from "child_process";
var MAC_CANDIDATES = [
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
  "/Applications/Chromium.app/Contents/MacOS/Chromium",
  "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
  "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
  "/Applications/Arc.app/Contents/MacOS/Arc"
];
var LINUX_CANDIDATES = [
  "/usr/bin/google-chrome",
  "/usr/bin/google-chrome-stable",
  "/usr/bin/chromium",
  "/usr/bin/chromium-browser",
  "/usr/bin/microsoft-edge",
  "/snap/bin/chromium"
];
var WIN_CANDIDATES = [
  "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
  "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
  "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe",
  "C:\\Program Files\\Chromium\\Application\\chrome.exe"
];
function findChrome(override) {
  if (override) return existsSync2(override) ? override : void 0;
  const env = process.env.PLUMB_CHROME ?? process.env.CHROME_PATH;
  if (env && existsSync2(env)) return env;
  const candidates = process.platform === "darwin" ? MAC_CANDIDATES : process.platform === "win32" ? WIN_CANDIDATES : LINUX_CANDIDATES;
  for (const path of candidates) {
    if (existsSync2(path)) return path;
  }
  const which = process.platform === "win32" ? "where" : "which";
  for (const cmd of ["google-chrome", "chromium", "chromium-browser", "chrome", "microsoft-edge"]) {
    try {
      const out = execSync(`${which} ${cmd}`, { stdio: ["ignore", "pipe", "ignore"] }).toString().trim().split(/\r?\n/)[0];
      if (out && existsSync2(out)) return out;
    } catch {
    }
  }
  return void 0;
}
function chromeInstallHint() {
  const platformHint = process.platform === "darwin" ? "Install Google Chrome from https://www.google.com/chrome/, or `brew install --cask google-chrome`." : process.platform === "win32" ? "Install Google Chrome from https://www.google.com/chrome/." : "Install Chromium: `sudo apt install chromium-browser` or `sudo dnf install chromium`.";
  return "Could not find a Chrome-compatible browser on this machine.\n" + platformHint + "\nIf Chrome is installed in a non-standard location, pass --chrome /path/to/chrome or set PLUMB_CHROME=/path/to/chrome.";
}

// src/cli/cdp.ts
import { spawn } from "child_process";
import { mkdtempSync, rmSync } from "fs";
import { tmpdir as tmpdir2 } from "os";
import { join as join5 } from "path";
import WebSocket from "ws";
async function launchBrowser(opts) {
  const userDataDir = mkdtempSync(join5(tmpdir2(), "plumb-chrome-"));
  const flags = [
    "--headless=new",
    "--disable-gpu",
    "--no-first-run",
    "--no-default-browser-check",
    "--remote-debugging-port=0",
    `--user-data-dir=${userDataDir}`,
    // Locked-down sandboxes (CI, Docker) reject --enable-features without
    // --no-sandbox; users on a desktop don't need this but it's harmless.
    "--no-sandbox",
    ...opts.extraFlags ?? [],
    "about:blank"
  ];
  const child = spawn(opts.chromePath, flags, { stdio: ["ignore", "pipe", "pipe"] });
  let wsUrl;
  const stderrBuf = [];
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new Error(`Chrome failed to print a DevTools URL within 15s. stderr:
${stderrBuf.join("")}`));
    }, 15e3);
    if (!child.stderr) {
      clearTimeout(timer);
      reject(new Error("Chrome stderr stream missing"));
      return;
    }
    child.stderr.on("data", (chunk) => {
      const s = chunk.toString("utf8");
      stderrBuf.push(s);
      if (opts.verbose) process.stderr.write(`[chrome] ${s}`);
      const m = /DevTools listening on (ws:\/\/\S+)/.exec(s);
      if (m && m[1]) {
        wsUrl = m[1];
        clearTimeout(timer);
        resolve();
      }
    });
    child.on("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`Chrome exited (code ${code}). stderr:
${stderrBuf.join("")}`));
    });
  });
  if (!wsUrl) throw new Error("Chrome printed no DevTools URL");
  const httpJson = wsUrl.replace(/^ws:\/\//, "http://").replace(/\/devtools\/.*$/, "/json/list");
  const pageWs = await findPageTarget(httpJson);
  const ws = await connect(pageWs);
  let nextId = 1;
  const pending2 = /* @__PURE__ */ new Map();
  const handlers = /* @__PURE__ */ new Map();
  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (typeof msg.id === "number") {
      const p = pending2.get(msg.id);
      if (!p) return;
      pending2.delete(msg.id);
      if (msg.error) p.reject(new Error(`CDP ${msg.id}: ${msg.error.message}`));
      else p.resolve(msg.result);
      return;
    }
    if (msg.method) {
      const set = handlers.get(msg.method);
      if (!set) return;
      for (const h of set) {
        try {
          h(msg.params ?? {});
        } catch {
        }
      }
    }
  });
  function send2(method, params) {
    const id = nextId++;
    return new Promise((resolve, reject) => {
      pending2.set(id, { resolve, reject });
      ws.send(JSON.stringify({ id, method, params: params ?? {} }));
    });
  }
  function on(event, handler) {
    let set = handlers.get(event);
    if (!set) {
      set = /* @__PURE__ */ new Set();
      handlers.set(event, set);
    }
    set.add(handler);
    return () => set?.delete(handler);
  }
  function waitFor(event, predicate, timeoutMs = 3e4) {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        off();
        reject(new Error(`Timed out waiting for ${event}`));
      }, timeoutMs);
      const off = on(event, (p) => {
        if (predicate && !predicate(p)) return;
        clearTimeout(timer);
        off();
        resolve(p);
      });
    });
  }
  async function close() {
    try {
      ws.close();
    } catch {
    }
    try {
      child.kill("SIGTERM");
    } catch {
    }
    try {
      rmSync(userDataDir, { recursive: true, force: true });
    } catch {
    }
  }
  return { send: send2, on, waitFor, close };
}
async function findPageTarget(httpJsonUrl) {
  for (let i = 0; i < 30; i++) {
    try {
      const res = await fetch(httpJsonUrl);
      if (res.ok) {
        const list = await res.json();
        const page = list.find((t) => t.type === "page" && !!t.webSocketDebuggerUrl);
        if (page?.webSocketDebuggerUrl) return page.webSocketDebuggerUrl;
      }
    } catch {
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`No Chrome page target available at ${httpJsonUrl}`);
}
function connect(wsUrl) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl, { perMessageDeflate: false });
    ws.once("open", () => resolve(ws));
    ws.once("error", reject);
  });
}
async function navigate(b, url, waitMs = 0) {
  await b.send("Page.enable");
  await b.send("Runtime.enable");
  const loaded = b.waitFor("Page.loadEventFired", void 0, 3e4);
  await b.send("Page.navigate", { url });
  await loaded;
  if (waitMs > 0) await new Promise((r) => setTimeout(r, waitMs));
}
async function evaluate(b, expression) {
  const res = await b.send("Runtime.evaluate", {
    expression,
    returnByValue: true,
    awaitPromise: true
  });
  if (res.exceptionDetails) {
    throw new Error(
      `Page-side error: ${res.exceptionDetails.exception?.description ?? "unknown"}`
    );
  }
  return res.result?.value;
}

// src/fit.ts
var ERR_WEIGHT = 1;
var WARN_WEIGHT = 0.35;
var EXTRA_STEP = 0.02;
var EXTRA_CAP = 0.15;
var DEFAULT_ACCEPT = 98;
function scoreOf(result) {
  const cov = result.coverage;
  const importantTotal = cov?.importantTotal ?? cov?.pdsTotal ?? result.matched;
  const importantMatched = cov?.importantMatched ?? result.matched;
  const coverageFactor = importantTotal > 0 ? clamp(importantMatched / importantTotal, 0, 1) : result.matched > 0 ? 1 : 0;
  const errEls = /* @__PURE__ */ new Set();
  const warnEls = /* @__PURE__ */ new Set();
  for (const d of result.deltas) {
    if (d.kind === "missing-in-pds") continue;
    if (d.severity === "error") errEls.add(d.el);
    else if (d.severity === "warn") warnEls.add(d.el);
  }
  for (const el of errEls) warnEls.delete(el);
  let fidelity;
  if (result.matched <= 0) {
    fidelity = 0;
  } else {
    const penalised = ERR_WEIGHT * errEls.size + WARN_WEIGHT * warnEls.size;
    fidelity = clamp((result.matched - penalised) / result.matched, 0, 1);
    const extras = Math.max(0, result.unmatched);
    fidelity = clamp(fidelity - Math.min(EXTRA_CAP, extras * EXTRA_STEP), 0, 1);
  }
  const score = Math.round(coverageFactor * fidelity * 1e3) / 10;
  return {
    score,
    coverageFactor,
    fidelity,
    importantTotal,
    importantMatched,
    errors: errEls.size,
    warns: warnEls.size
  };
}
function buildFitResponse(result, opts = {}) {
  const accept = opts.accept ?? DEFAULT_ACCEPT;
  const maxFixes = opts.maxFixes ?? 8;
  const s = scoreOf(result);
  const done = result.ok && s.score >= accept;
  const topFixes = result.deltas.filter((d) => d.severity !== "info" && d.kind !== "missing-in-pds").slice(0, maxFixes).map(fmtFix);
  const pct = s.score.toFixed(1);
  let instruction;
  if (done) {
    instruction = `Pixel-perfect \u2014 ${pct}% (${s.importantMatched}/${s.importantTotal} key nodes built, ${result.deltas.length} note(s) within tolerance). Stop: the build matches the design.`;
  } else if (result.matched === 0) {
    instruction = `Nothing matched yet (0%). Render the component, stamp data-plumb-id="<el>" on each element using the PDS handles, capture box + computed styles, and call plumb_fit again.`;
  } else {
    const it = opts.iteration ? `Iteration ${opts.iteration}: ` : "";
    const missing = s.importantTotal - s.importantMatched;
    const coverHint = missing > 0 ? ` Then build the ${missing} key node(s) you haven't tagged yet \u2014 that's capping your score.` : "";
    const errPart = s.errors > 0 ? `${s.errors} error(s)` : "";
    const warnPart = s.warns > 0 ? `${s.warns} warning(s)` : "";
    const both = [errPart, warnPart].filter(Boolean).join(" and ");
    instruction = `${it}${pct}% \u2014 fix ${both || "the deltas"} below, re-render, and call plumb_fit again.${coverHint}`;
  }
  return {
    score: s.score,
    done,
    iteration: opts.iteration,
    matched: result.matched,
    importantMatched: s.importantMatched,
    importantTotal: s.importantTotal,
    errors: s.errors,
    warns: s.warns,
    bar: renderBar(s.score),
    topFixes,
    instruction
  };
}
function fmtFix(d) {
  const where = d.name ? `${d.el} (${d.name})` : d.el;
  return `${where} \xB7 ${d.kind}: expected ${fmtVal(d.expected)}, got ${fmtVal(d.actual)}`;
}
function fmtVal(v) {
  if (v === null) return "\u2014";
  return typeof v === "string" ? `"${v}"` : String(v);
}
function renderBar(score, width = 10) {
  const filled = Math.round(clamp(score, 0, 100) / 100 * width);
  return "\u25B0".repeat(filled) + "\u25B1".repeat(Math.max(0, width - filled));
}
function clamp(n, lo, hi) {
  return Math.max(lo, Math.min(hi, n));
}

// src/fit/generate.ts
var DEFAULT_FIT_MODEL = "claude-sonnet-4-6";
var ANTHROPIC_URL = "https://api.anthropic.com/v1/messages";
var ANTHROPIC_VERSION = "2023-06-01";
var MAX_TOKENS = 16e3;
var GENERATOR_SYSTEM_PROMPT = `You are Plumb's pixel-fitting code generator. You convert a Plumb Design Spec (PDS) \u2014 a compact, normalized JSON description of a Figma node tree \u2014 into ONE self-contained HTML document that renders pixel-identically to the design, and you iteratively correct it against measured deltas.

OUTPUT CONTRACT (non-negotiable):
- Output ONE complete HTML document and NOTHING else. No markdown fences, no prose, no explanation. The first characters must be "<!doctype html>".
- Load Tailwind via <script src="https://cdn.tailwindcss.com"></script> in <head>. You may add an inline tailwind.config <script> for custom colors/fonts.
- On EVERY rendered element that corresponds to a PDS node, set data-plumb-id="<el>" using that node's exact \`el\` handle. This is how your output is measured: an element without the correct data-plumb-id cannot be scored, and an unbuilt key node caps the score.
- The root element is the PDS root node, sized to its box (w\xD7h). Render at the design's own dimensions on a plain white (or design-specified) body.

FIDELITY RULES:
- Colors: use the exact hex from the token table (tokens.color, keys like $c1). Node \`fill\` on a non-text node \u2192 background-color; \`fill\` on a text node \u2192 color; \`stroke\` \u2192 border-color. Resolve $-refs through the token table before use.
- Layout: from \`layout\` \u2014 flow ("row"|"col") \u2192 flex-direction (row|column); gap \u2192 gap; pad [t,r,b,l] \u2192 padding; justify \u2192 justify-content; align \u2192 align-items. Honor flex-child sizing: grow \u2192 flex-grow; selfAlign (stretch|min|max|center) \u2192 align-self; layoutSizing FILL \u2192 flex:1 / stretch, HUG \u2192 fit-content.
- Typography: tokens.text values look like "600 14px/20 Inter" = weight size/line-height family. Match font-size, font-weight, line-height (as a unitless ratio or px), and use the named family with sensible system fallbacks.
- radius: tokens.radius; the literal "full" \u2192 border-radius:9999px. strokeW \u2192 border-width (with the stroke color). Reproduce shadows (tokens.shadow), opacity, rotation (transform), and textDecoration.
- Text: use node.chars verbatim as the element's text. Respect textCase (UPPER/LOWER/TITLE) if present.
- Images / icons / logos are ASSETS \u2014 use the real thing, never redraw them as CSS. A node with \`assetId\` is a raster export: render an <img> pointing at the exported file and stamp data-plumb-asset="<assetId>" on it. A node with \`vectorPath\` (or type "vector") is a vector: render an inline <svg> using the provided path \`d\` string(s) \u2014 do NOT approximate a logo/glyph with CSS borders, gradients, or shapes. Only when no asset/vector/path is available may you fall back to a correctly-sized neutral placeholder (still tagged with data-plumb-id). Verify flags a visual node rendered without real image/vector content as an error, so a redrawn logo will tank the score \u2014 it should.

CORRECTION MODE:
- When given PREVIOUS_HTML and DELTAS, return a corrected FULL document. Each delta is { el, kind, expected, actual }: change the element whose data-plumb-id === el so its measured value becomes \`expected\`. Delta kinds include size.w, size.h, layout.gap, pad.top/right/bottom/left, layout.flow, fill, text.color, text.chars, text.size, text.weight, text.lh, text.family, radius, stroke.width, opacity, shadow.missing, flex.grow, flex.selfAlign. Fix exactly the listed deltas and DO NOT regress elements that were already correct. Re-output the entire document.`;
function buildUserMessage(input) {
  const pdsJson = JSON.stringify(input.pds);
  if (!input.prevHtml || !input.deltas || input.deltas.length === 0) {
    return `PDS:
${pdsJson}

Generate the full HTML document now.`;
  }
  const fixes = input.deltas.slice(0, 40).map(
    (d) => `- ${d.el} \xB7 ${d.kind}: expected ${fmt(d.expected)}, got ${fmt(d.actual)}`
  ).join("\n");
  return `PDS:
${pdsJson}

PREVIOUS_HTML:
${input.prevHtml}

DELTAS (fix exactly these, do not regress what is already correct):
${fixes}

Return the corrected full HTML document.`;
}
function fmt(v) {
  if (v === null) return "\u2014";
  return typeof v === "string" ? `"${v}"` : String(v);
}
function extractHtml(raw) {
  let s = raw.trim();
  const fence = s.match(/```(?:html)?\s*([\s\S]*?)```/i);
  if (fence && fence[1]) s = fence[1].trim();
  const start = s.search(/<!doctype html|<html/i);
  if (start > 0) s = s.slice(start);
  return s.trim();
}
async function generateHtml(input) {
  const model = input.model ?? DEFAULT_FIT_MODEL;
  const res = await fetch(ANTHROPIC_URL, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": input.apiKey,
      "anthropic-version": ANTHROPIC_VERSION
    },
    body: JSON.stringify({
      model,
      max_tokens: MAX_TOKENS,
      system: GENERATOR_SYSTEM_PROMPT,
      messages: [{ role: "user", content: buildUserMessage(input) }]
    }),
    signal: input.signal
  });
  if (!res.ok) {
    let detail = `${res.status} ${res.statusText}`;
    try {
      const body2 = await res.json();
      if (body2.error) detail = `${body2.error.type}: ${body2.error.message}`;
    } catch {
    }
    if (res.status === 401) {
      throw new Error(`Anthropic auth failed (${detail}). Check ANTHROPIC_API_KEY.`);
    }
    throw new Error(`Anthropic request failed \u2014 ${detail}.`);
  }
  const body = await res.json();
  const text = (body.content ?? []).filter((b) => b.type === "text" && typeof b.text === "string").map((b) => b.text).join("");
  const html = extractHtml(text);
  if (!html || !/<html|<!doctype/i.test(html)) {
    throw new Error("Generator returned no usable HTML document.");
  }
  return html;
}

// src/fit/serve.ts
import { createServer as createServer2 } from "http";
function serveHtml(html) {
  return new Promise((resolve, reject) => {
    const server = createServer2((_req, res) => {
      res.writeHead(200, {
        "content-type": "text/html; charset=utf-8",
        "cache-control": "no-store"
      });
      res.end(html);
    });
    server.on("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (!addr || typeof addr === "string") {
        server.close();
        reject(new Error("Failed to bind the local render server."));
        return;
      }
      resolve({
        url: `http://127.0.0.1:${addr.port}/`,
        close: () => new Promise((res) => {
          server.close(() => res());
        })
      });
    });
  });
}

// src/figma/rest.ts
async function fetchNodeViaRest(req) {
  const { fileKey, nodeId: nodeId2, depth, token } = req;
  const url = `https://api.figma.com/v1/files/${encodeURIComponent(fileKey)}/nodes?ids=${encodeURIComponent(nodeId2)}&depth=${depth}`;
  let res;
  try {
    res = await fetch(url, { headers: { "X-Figma-Token": token } });
  } catch (e) {
    throw new PlumbError(
      `Could not reach api.figma.com: ${e.message}`,
      "Check your internet connection, then retry."
    );
  }
  if (!res.ok) throw restError(res.status, fileKey);
  const json = await res.json();
  const entry = json.nodes?.[nodeId2];
  if (!entry?.document) {
    throw new PlumbError(
      `Figma returned no node "${nodeId2}" for file "${fileKey}".`,
      "Check the node id \u2014 copy the part after 'node-id=' in the Figma URL and replace '-' with ':'. Confirm it belongs to this file."
    );
  }
  return {
    document: entry.document,
    fileName: json.name ?? "",
    version: String(json.version ?? "")
  };
}
async function fetchFileOutline(fileKey, token, depth = 2) {
  const url = `https://api.figma.com/v1/files/${encodeURIComponent(fileKey)}?depth=${depth}`;
  let res;
  try {
    res = await fetch(url, { headers: { "X-Figma-Token": token } });
  } catch (e) {
    throw new PlumbError(
      `Could not reach api.figma.com: ${e.message}`,
      "Check your internet connection, then retry."
    );
  }
  if (!res.ok) throw restError(res.status, fileKey);
  const json = await res.json();
  if (!json.document) {
    throw new PlumbError(
      `Figma returned no document for file "${fileKey}".`,
      "Check the file key \u2014 the string after /design/ or /file/ in the Figma URL."
    );
  }
  return {
    document: json.document,
    fileName: json.name ?? "",
    version: String(json.version ?? "")
  };
}
function restError(status, fileKey) {
  switch (status) {
    case 401:
    case 403:
      return new PlumbError(
        `Figma rejected the request (HTTP ${status}).`,
        "Set a valid token in the FIGMA_TOKEN env var. Create a read-only token at figma.com \u2192 Settings \u2192 Security \u2192 personal access tokens."
      );
    case 404:
      return new PlumbError(
        `Figma file "${fileKey}" was not found (HTTP 404).`,
        "Check the file key \u2014 it is the string after '/design/' or '/file/' in the Figma URL."
      );
    case 429:
      return new PlumbError(
        "Figma rate-limited the request (HTTP 429).",
        "Wait, then retry. REST limits are very low on free/Starter workspaces \u2014 this is exactly why Plumb's primary path is the plugin, not REST."
      );
    default:
      return new PlumbError(
        `Figma REST request failed (HTTP ${status}).`,
        "Retry; if it persists, check status.figma.com."
      );
  }
}

// src/figma/url.ts
var PATH_RE = /^\/(?:design|file|proto)\/([^/]+)(?:\/branch\/([^/]+))?/;
function parseFigmaUrl(input) {
  if (typeof input !== "string" || input.length === 0) return null;
  const text = input.trim();
  if (text.length === 0) return null;
  const withProtocol = /^https?:\/\//.test(text) ? text : `https://${text}`;
  let url;
  try {
    url = new URL(withProtocol);
  } catch {
    return null;
  }
  if (!url.hostname.endsWith("figma.com")) return null;
  const m = PATH_RE.exec(url.pathname);
  if (!m) return null;
  const fileKey = m[1];
  if (!fileKey) return null;
  const branchKey = m[2];
  const rawNodeId = url.searchParams.get("node-id");
  const nodeId2 = rawNodeId ? rawNodeId.replace(/-/g, ":") : void 0;
  const out = { fileKey };
  if (nodeId2) out.nodeId = nodeId2;
  if (branchKey) out.branchKey = branchKey;
  return out;
}
function resolveFigmaTarget(args) {
  if (!args.url) {
    return { fileKey: args.fileKey, id: args.id };
  }
  const parsed = parseFigmaUrl(args.url);
  if (!parsed) {
    return { fileKey: args.fileKey, id: args.id };
  }
  return {
    fileKey: args.fileKey ?? parsed.fileKey,
    id: args.id ?? parsed.nodeId,
    branchKey: parsed.branchKey
  };
}

// src/normalize/handles.ts
var HandleMinter = class {
  used = /* @__PURE__ */ new Set();
  mint(name, type) {
    const base = slug(name) || slug(type) || "node";
    let el = base;
    let n = 2;
    while (this.used.has(el)) el = `${base}-${n++}`;
    this.used.add(el);
    return el;
  }
};
function slug(s) {
  return s.toLowerCase().normalize("NFKD").replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 40).replace(/-+$/g, "");
}

// src/util/num.ts
function round(n, places = 0) {
  const f = 10 ** places;
  return Math.round(n * f) / f;
}
function cleanPx(n) {
  if (!Number.isFinite(n)) return 0;
  const r = Math.round(n * 100) / 100;
  return Math.abs(r) < 0.01 ? 0 : r;
}
function clamp01(n) {
  if (n < 0) return 0;
  if (n > 1) return 1;
  return n;
}

// src/normalize/layout.ts
var JUSTIFY = {
  MIN: "flex-start",
  CENTER: "center",
  MAX: "flex-end",
  SPACE_BETWEEN: "space-between"
};
var ALIGN = {
  MIN: "flex-start",
  CENTER: "center",
  MAX: "flex-end",
  BASELINE: "baseline"
};
function toLayout(n) {
  if (!n.layoutMode || n.layoutMode === "NONE") return void 0;
  const flow = n.layoutMode === "VERTICAL" ? "col" : "row";
  const layout = {
    flow,
    pad: [
      cleanPx(n.paddingTop ?? 0),
      cleanPx(n.paddingRight ?? 0),
      cleanPx(n.paddingBottom ?? 0),
      cleanPx(n.paddingLeft ?? 0)
    ]
  };
  if (n.itemSpacing) {
    const gap = cleanPx(n.itemSpacing);
    if (gap) layout.gap = gap;
  }
  if (n.layoutWrap === "WRAP") {
    layout.wrap = true;
    if (n.counterAxisSpacing) {
      const gapCross = cleanPx(n.counterAxisSpacing);
      if (gapCross) layout.gapCross = gapCross;
    }
  }
  const primary = n.primaryAxisAlignItems;
  if (primary && primary !== "MIN" && JUSTIFY[primary]) {
    layout.justify = JUSTIFY[primary];
  }
  const counter2 = n.counterAxisAlignItems;
  if (counter2 && counter2 !== "MIN" && ALIGN[counter2]) {
    layout.align = ALIGN[counter2];
  }
  return layout;
}

// src/normalize/paint.ts
function channel(x) {
  return Math.round(clamp01(x) * 255).toString(16).padStart(2, "0");
}
function colorToHex(c) {
  const hex = `#${channel(c.r)}${channel(c.g)}${channel(c.b)}`;
  const a = c.a ?? 1;
  return a >= 1 ? hex : hex + channel(a);
}
function visiblePaints(paints) {
  if (!paints?.length) return [];
  return paints.filter((p) => p && p.visible !== false && (p.opacity ?? 1) > 0);
}
function paintToCss(paints) {
  const list = visiblePaints(paints);
  if (!list.length) return void 0;
  const p = list[0];
  if (p.type === "SOLID" && p.color) {
    const a = (p.color.a ?? 1) * (p.opacity ?? 1);
    return colorToHex({ r: p.color.r, g: p.color.g, b: p.color.b, a });
  }
  if (p.type.startsWith("GRADIENT")) return "gradient";
  if (p.type === "IMAGE") return "image";
  return void 0;
}
var GRADIENT_KIND = {
  GRADIENT_LINEAR: "linear-gradient",
  GRADIENT_RADIAL: "radial-gradient",
  GRADIENT_ANGULAR: "angular-gradient",
  GRADIENT_DIAMOND: "diamond-gradient"
};
var SCALE_MODE = {
  FILL: "fill",
  FIT: "fit",
  STRETCH: "stretch",
  CROP: "crop",
  TILE: "tile"
};
function gradientAngle(handles) {
  if (!handles || handles.length < 2) return void 0;
  const [a, b] = handles;
  if (!a || !b) return void 0;
  const dx = b.x - a.x;
  const dy = b.y - a.y;
  if (dx === 0 && dy === 0) return void 0;
  let deg = Math.atan2(dy, dx) * 180 / Math.PI + 90;
  if (deg < 0) deg += 360;
  if (deg >= 360) deg -= 360;
  return round(deg, 1);
}
function stopFor(s) {
  return { at: round(s.position, 4), color: colorToHex(s.color) };
}
function fillOf(p) {
  if (p.type === "SOLID" && p.color) {
    const out = {
      type: "color",
      color: colorToHex({ r: p.color.r, g: p.color.g, b: p.color.b, a: p.color.a ?? 1 })
    };
    if (p.opacity !== void 0 && p.opacity < 1) out.opacity = round(p.opacity, 2);
    if (p.var) out.var = p.var;
    return out;
  }
  const grad = GRADIENT_KIND[p.type];
  if (grad) {
    const out = {
      type: grad,
      stops: (p.gradientStops ?? []).map(stopFor)
    };
    if (grad === "linear-gradient") {
      const ang = gradientAngle(p.gradientHandlePositions);
      if (ang !== void 0) out.angle = ang;
    }
    if (p.opacity !== void 0 && p.opacity < 1) out.opacity = round(p.opacity, 2);
    return out;
  }
  if (p.type === "IMAGE") {
    const out = { type: "image" };
    if (p.imageRef) out.assetId = p.imageRef;
    if (p.scaleMode && SCALE_MODE[p.scaleMode]) out.scaleMode = SCALE_MODE[p.scaleMode];
    if (p.opacity !== void 0 && p.opacity < 1) out.opacity = round(p.opacity, 2);
    if (Array.isArray(p.imageTransform)) out.crop = p.imageTransform;
    if (typeof p.rotation === "number" && Math.abs(p.rotation) > 1e-4) {
      out.rotation = round(p.rotation, 2);
    }
    return out;
  }
  return void 0;
}
function paintsToFillStack(paints) {
  const list = visiblePaints(paints);
  if (!list.length) return void 0;
  if (list.length === 1 && list[0].type === "SOLID" && !list[0].var) return void 0;
  const out = [];
  for (const p of list) {
    const f = fillOf(p);
    if (f) out.push(f);
  }
  return out.length ? out : void 0;
}
function typeStyleToCss(s) {
  if (!s || s.fontSize == null) return void 0;
  const weight = s.fontWeight ?? 400;
  const size = round(s.fontSize, 2);
  const lh = s.lineHeightPx != null && s.fontSize ? `/${round(s.lineHeightPx / s.fontSize, 2)}` : "";
  const family = s.fontFamily ? ` ${s.fontFamily}` : "";
  const tracking = s.letterSpacing ? ` ${round(s.letterSpacing, 2)}px` : "";
  return `${weight} ${size}px${lh}${family}${tracking}`;
}
function visibleEffects(effects) {
  if (!effects?.length) return [];
  return effects.filter((e) => e && e.visible !== false);
}
function effectsToCss(effects) {
  const shadows = visibleEffects(effects).filter(
    (e) => e.type === "DROP_SHADOW" || e.type === "INNER_SHADOW"
  );
  if (!shadows.length) return void 0;
  return shadows.map((e) => {
    const inset = e.type === "INNER_SHADOW" ? "inset " : "";
    const ox = round(e.offset?.x ?? 0, 2);
    const oy = round(e.offset?.y ?? 0, 2);
    const blur = round(e.radius ?? 0, 2);
    const spread = round(e.spread ?? 0, 2);
    const col = e.color ? colorToHex(e.color) : "#00000000";
    return `${inset}${ox}px ${oy}px ${blur}px ${spread}px ${col}`;
  }).join(", ");
}
function effectsToStack(effects) {
  const list = visibleEffects(effects);
  if (!list.length) return void 0;
  const onlyShadow = list.length === 1 && (list[0].type === "DROP_SHADOW" || list[0].type === "INNER_SHADOW");
  if (onlyShadow) return void 0;
  const out = [];
  for (const e of list) {
    if (e.type === "DROP_SHADOW" || e.type === "INNER_SHADOW") {
      out.push({
        type: e.type === "DROP_SHADOW" ? "drop-shadow" : "inner-shadow",
        x: round(e.offset?.x ?? 0, 2),
        y: round(e.offset?.y ?? 0, 2),
        blur: round(e.radius ?? 0, 2),
        spread: round(e.spread ?? 0, 2),
        color: e.color ? colorToHex(e.color) : "#00000000"
      });
    } else if (e.type === "LAYER_BLUR") {
      out.push({ type: "layer-blur", radius: round(e.radius ?? 0, 2) });
    } else if (e.type === "BACKGROUND_BLUR") {
      out.push({ type: "background-blur", radius: round(e.radius ?? 0, 2) });
    }
  }
  return out.length ? out : void 0;
}
function backdropFilterCss(effects) {
  const list = visibleEffects(effects);
  const bb = list.find((e) => e.type === "BACKGROUND_BLUR");
  if (!bb) return void 0;
  return `blur(${round(bb.radius ?? 0, 2)}px)`;
}

// src/normalize/tokens.ts
var TokenInterner = class {
  color = /* @__PURE__ */ new Map();
  text = /* @__PURE__ */ new Map();
  radius = /* @__PURE__ */ new Map();
  shadow = /* @__PURE__ */ new Map();
  // Compound namespaces (v0.10).
  layout = /* @__PURE__ */ new Map();
  effects = /* @__PURE__ */ new Map();
  fills = /* @__PURE__ */ new Map();
  vector = /* @__PURE__ */ new Map();
  props = /* @__PURE__ */ new Map();
  // Per-ref hit count — surfaced as tokens.meta.counts so agents can see
  // "$l1 used 23×" as concrete proof of the dedup. Cheap; ~one int per ref.
  counts = /* @__PURE__ */ new Map();
  internColor(paints) {
    const v = paintToCss(paints);
    return v === void 0 ? void 0 : this.intern(this.color, "c", v);
  }
  internText(style) {
    const v = typeStyleToCss(style);
    return v === void 0 ? void 0 : this.intern(this.text, "t", v);
  }
  internRadius(r) {
    if (r === void 0) return void 0;
    if (r === "full") return this.intern(this.radius, "r", "full");
    if (r <= 0) return void 0;
    return this.intern(this.radius, "r", String(r));
  }
  internShadow(effects) {
    const v = effectsToCss(effects);
    return v === void 0 ? void 0 : this.intern(this.shadow, "s", v);
  }
  /** Intern a full PdsLayout object — fires per auto-layout container. */
  internLayout(layout) {
    if (!layout) return void 0;
    return this.intern(this.layout, "l", stableStringify(layout));
  }
  /** Intern an effects stack — fires on multi-shadow / blur surfaces. */
  internEffects(stack) {
    if (!stack || stack.length === 0) return void 0;
    return this.intern(this.effects, "e", stableStringify(stack));
  }
  /** Intern a full fills stack — gradients, layered solids, image fills. */
  internFills(stack) {
    if (!stack || stack.length === 0) return void 0;
    return this.intern(this.fills, "f", stableStringify(stack));
  }
  /** Intern a vector path `d` string — fires on every drawn icon. */
  internVector(path) {
    if (!path) return void 0;
    return this.intern(this.vector, "v", path);
  }
  /** Intern a component-instance prop map. */
  internProps(props) {
    if (!props || Object.keys(props).length === 0) return void 0;
    return this.intern(this.props, "p", stableStringify(props));
  }
  intern(map, prefix, value) {
    const existing = map.get(value);
    if (existing) {
      this.counts.set(existing, (this.counts.get(existing) ?? 1) + 1);
      return existing;
    }
    const id = `$${prefix}${map.size + 1}`;
    map.set(value, id);
    this.counts.set(id, 1);
    return id;
  }
  table() {
    const out = {
      color: invert(this.color),
      text: invert(this.text),
      radius: Object.fromEntries(
        [...this.radius].map(([value, id]) => [
          id,
          value === "full" ? "full" : Number(value)
        ])
      ),
      shadow: invert(this.shadow)
    };
    if (this.layout.size > 0) out.layout = parseInvert(this.layout);
    if (this.effects.size > 0) out.effects = parseInvert(this.effects);
    if (this.fills.size > 0) out.fills = parseInvert(this.fills);
    if (this.vector.size > 0) out.vector = invert(this.vector);
    if (this.props.size > 0) {
      out.props = parseInvert(this.props);
    }
    const counts = {};
    for (const [id, n] of this.counts) if (n >= 2) counts[id] = n;
    if (Object.keys(counts).length > 0) out.meta = { counts };
    return out;
  }
};
function invert(map) {
  const out = {};
  for (const [value, id] of map) out[id] = value;
  return out;
}
function parseInvert(map) {
  const out = {};
  for (const [value, id] of map) out[id] = JSON.parse(value);
  return out;
}
function stableStringify(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) {
    return `[${value.map((v) => stableStringify(v)).join(",")}]`;
  }
  const keys = Object.keys(value).sort();
  const parts = keys.map(
    (k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`
  );
  return `{${parts.join(",")}}`;
}

// src/normalize/typo.ts
function editDistance(a, b, max) {
  if (a === b) return 0;
  if (Math.abs(a.length - b.length) > max) return max + 1;
  if (a.length === 0) return b.length;
  if (b.length === 0) return a.length;
  let prev = new Array(b.length + 1).fill(0);
  let cur = new Array(b.length + 1).fill(0);
  for (let j = 0; j <= b.length; j++) prev[j] = j;
  for (let i = 1; i <= a.length; i++) {
    cur[0] = i;
    let rowMin = i;
    for (let j = 1; j <= b.length; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      const v = Math.min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost);
      cur[j] = v;
      if (v < rowMin) rowMin = v;
    }
    if (rowMin > max) return max + 1;
    [prev, cur] = [cur, prev];
  }
  return prev[b.length];
}
function detectSuspiciousText(nodes) {
  const items = [];
  for (const el in nodes) {
    const n = nodes[el];
    if (!n || n.type !== "text") continue;
    const chars = typeof n.chars === "string" ? n.chars.trim() : "";
    if (!chars) continue;
    items.push({ path: n.path ?? n.el, chars });
  }
  if (items.length < 4) return [];
  const freq = /* @__PURE__ */ new Map();
  for (const t of items) freq.set(t.chars, (freq.get(t.chars) ?? 0) + 1);
  const out = [];
  for (const t of items) {
    if ((freq.get(t.chars) ?? 0) !== 1) continue;
    let bestMatch;
    for (const [other, count] of freq) {
      if (other === t.chars) continue;
      if (count < 3) continue;
      const max = t.chars.length > 12 ? 2 : 1;
      const d = editDistance(t.chars, other, max);
      if (d > 0 && d <= max) {
        if (!bestMatch || count > bestMatch.count) {
          bestMatch = { chars: other, count };
        }
      }
    }
    if (bestMatch) {
      out.push({
        path: t.path,
        value: t.chars,
        hint: `Possible typo: "${bestMatch.chars}" appears ${bestMatch.count}\xD7 nearby`
      });
    }
  }
  return out;
}

// src/util/estimate.ts
function estimateTokens(text) {
  return Math.ceil(text.length / 4);
}

// src/normalize/normalize.ts
var TYPE_MAP = {
  FRAME: "frame",
  GROUP: "group",
  TEXT: "text",
  RECTANGLE: "rect",
  ELLIPSE: "ellipse",
  LINE: "line",
  VECTOR: "vector",
  STAR: "star",
  POLYGON: "polygon",
  INSTANCE: "instance",
  COMPONENT: "component",
  COMPONENT_SET: "component-set",
  BOOLEAN_OPERATION: "bool",
  SECTION: "section"
};
function buildPreWalk(file) {
  const minter = new HandleMinter();
  const elById = /* @__PURE__ */ new Map();
  function preWalk(fn, level) {
    if (level > 0 && fn.visible === false) return;
    elById.set(fn.id, minter.mint(fn.name ?? "", fn.type ?? ""));
    const kids = (fn.children ?? []).filter((k) => k.visible !== false);
    for (const child of kids) preWalk(child, level + 1);
  }
  preWalk(file.document, 0);
  return { elById, minter };
}
function normalize2(file, depth, opts = {}) {
  const interner = new TokenInterner();
  const nodes = {};
  const { elById, minter } = opts.precomputed ?? buildPreWalk(file);
  const SURFACE_TYPES = /* @__PURE__ */ new Set(["frame", "group", "rect", "instance", "component"]);
  function walk(fn, level, parent, parentPath, inheritedFill, grandparent, ancestors = []) {
    if (level > 0 && fn.visible === false) return void 0;
    const el = elById.get(fn.id) ?? minter.mint(fn.name ?? "", fn.type ?? "");
    const path = parentPath ? `${parentPath}.${el}` : el;
    const node = {
      id: fn.id,
      el,
      type: TYPE_MAP[fn.type] ?? fn.type.toLowerCase(),
      box: {
        w: round(fn.absoluteBoundingBox?.width ?? 0),
        h: round(fn.absoluteBoundingBox?.height ?? 0)
      }
    };
    if (fn.name && isDescriptive(fn.name)) node.name = fn.name;
    if (parentPath) node.path = path;
    const pos = relativePos(parent, fn);
    if (pos) node.pos = pos;
    const parentIsAutoLayout = !!(parent?.layoutMode && parent.layoutMode !== "NONE");
    if (parentIsAutoLayout) {
      if (typeof fn.layoutGrow === "number" && fn.layoutGrow > 0) {
        node.grow = fn.layoutGrow;
      }
      const selfAlign = normalizeLayoutAlign(fn.layoutAlign);
      if (selfAlign) node.selfAlign = selfAlign;
      const sizing = childSizing(fn);
      if (sizing) node.sizing = sizing;
    }
    const layout = toLayout(fn);
    if (layout) {
      if (layout.justify) {
        const visibleKids = (fn.children ?? []).filter((k) => k.visible !== false);
        if (visibleKids.length >= 2) {
          const isCol = layout.flow === "col";
          const sumMain = visibleKids.reduce((acc, k) => {
            const dim = isCol ? round(k.absoluteBoundingBox?.height ?? 0) : round(k.absoluteBoundingBox?.width ?? 0);
            return acc + dim;
          }, 0);
          const gapTotal = (visibleKids.length - 1) * (layout.gap ?? 0);
          const contentMain = sumMain + gapTotal;
          const containerMain = isCol ? node.box.h : node.box.w;
          const padStart = isCol ? layout.pad[0] : layout.pad[3];
          const padEnd = isCol ? layout.pad[2] : layout.pad[1];
          const available = containerMain - padStart - padEnd;
          if (available - contentMain >= 4) layout.contentMain = contentMain;
        }
      }
      node.layout = interner.internLayout(layout);
    }
    const fills = paintsToFillStack(fn.fills);
    const fill = interner.internColor(fn.fills);
    if (fills) {
      const firstImage = fills.find((f) => f.type === "image");
      if (firstImage && firstImage.assetId) node.assetId = firstImage.assetId;
      const hasImage = fills.some((f) => f.type === "image");
      node.fills = hasImage ? fills : interner.internFills(fills) ?? fills;
    } else if (fill) {
      node.fill = fill;
    }
    if (!fill && inheritedFill && SURFACE_TYPES.has(node.type)) {
      node.inheritedFill = inheritedFill;
    }
    const fillForChildren = fill ?? inheritedFill;
    const iconHint = inferIconHint(fn, parent, grandparent, ancestors, node);
    if (iconHint) node.iconHint = iconHint;
    const pattern = inferPattern(fn, node, layout);
    if (pattern) node.pattern = pattern;
    const stroke = interner.internColor(fn.strokes);
    if (stroke) {
      node.stroke = stroke;
      if (fn.strokeWeight) {
        const w = cleanPx(fn.strokeWeight);
        if (w) node.strokeW = w;
      }
      const align = normalizeStrokeAlign(fn.strokeAlign);
      if (align) node.strokeAlign = align;
      const sides = perSideStrokeWidths(fn, node.strokeW);
      if (sides) node.strokeSides = sides;
      if (Array.isArray(fn.dashPattern) && fn.dashPattern.length > 0) {
        node.strokeDash = fn.dashPattern.map((n) => round(n, 2));
      }
    }
    const radius = cornerRadius(fn, node.box);
    if (typeof radius === "number" || radius === "full") {
      const ref = interner.internRadius(radius);
      if (ref) node.radius = ref;
    } else if (radius) {
      node.radius = radius;
    }
    if (typeof fn.cornerRadiusVar === "string" && fn.cornerRadiusVar.length > 0) {
      node.radiusVar = fn.cornerRadiusVar;
    }
    const effects = effectsToStack(fn.effects);
    const shadow = interner.internShadow(fn.effects);
    if (effects) {
      node.effects = interner.internEffects(effects);
    } else if (shadow) {
      node.shadow = shadow;
    }
    const backdrop = backdropFilterCss(fn.effects);
    if (backdrop) node.backdropFilter = backdrop;
    if (fn.opacity != null && fn.opacity < 1) node.opacity = round(fn.opacity, 2);
    if (fn.clipsContent) node.clip = true;
    if (fn.isMask === true) {
      node.isMask = true;
      const mode = normalizeMaskType(fn.maskType);
      if (mode) node.maskMode = mode;
    }
    const inlined = inlineVectorPath(fn);
    if (inlined) node.vectorPath = interner.internVector(inlined);
    if (fn.type === "TEXT") {
      const text = interner.internText(fn.style);
      if (text) node.text = text;
      const dec = fn.style?.textDecoration;
      if (dec === "UNDERLINE") node.textDecoration = "underline";
      else if (dec === "STRIKETHROUGH") node.textDecoration = "line-through";
      const runs = fn.characterRuns;
      const dominantFill = node.fill;
      if (Array.isArray(runs) && runs.length > 1) {
        const builtRuns = runs.map((r) => {
          const sRef = interner.internText(r.style);
          const cRef = interner.internColor(r.fills);
          const runDec = r.style?.textDecoration;
          const dRun = runDec === "UNDERLINE" ? "underline" : runDec === "STRIKETHROUGH" ? "line-through" : void 0;
          const out = { t: r.characters };
          if (sRef && sRef !== text) out.s = sRef;
          if (cRef && cRef !== dominantFill) out.c = cRef;
          if (dRun && dRun !== node.textDecoration) out.d = dRun;
          return out;
        });
        node.chars = builtRuns;
      } else if (typeof fn.characters === "string") {
        node.chars = fn.characters;
      }
    }
    if (fn.type === "INSTANCE" && typeof fn.componentId === "string") {
      const vp = fn.variantProperties;
      if (vp && typeof vp === "object" && Object.keys(vp).length > 0) {
        const variant = Object.entries(vp).map(([k, v]) => `${k}=${v}`).join(",");
        node.component = { id: fn.componentId, variant };
      } else {
        node.component = fn.componentId;
      }
    }
    const props = normalizeComponentProps(fn.componentProperties);
    if (props) node.props = interner.internProps(props);
    if (typeof fn.rotation === "number") node.rotation = round(fn.rotation);
    if (typeof fn.blendMode === "string") node.blend = cssBlendMode(fn.blendMode);
    if (typeof fn.cornerSmoothing === "number") node.smooth = fn.cornerSmoothing;
    if (typeof fn.textAutoResize === "string") {
      node.textGrow = pdsTextGrow(fn.textAutoResize);
    }
    if (fn.constraints && !(parent?.layoutMode && parent.layoutMode !== "NONE")) {
      const h = pdsConstraint(fn.constraints.horizontal, "h");
      const v = pdsConstraint(fn.constraints.vertical, "v");
      if (h || v) node.constraints = { ...h && { h }, ...v && { v } };
    }
    const minSize = {};
    const maxSize = {};
    if (typeof fn.minWidth === "number") minSize.w = fn.minWidth;
    if (typeof fn.minHeight === "number") minSize.h = fn.minHeight;
    if (typeof fn.maxWidth === "number") maxSize.w = fn.maxWidth;
    if (typeof fn.maxHeight === "number") maxSize.h = fn.maxHeight;
    if (minSize.w !== void 0 || minSize.h !== void 0) node.sizingMin = minSize;
    if (maxSize.w !== void 0 || maxSize.h !== void 0) node.sizingMax = maxSize;
    const motion = motionFromReactions(fn.reactions);
    if (motion) node.motion = motion;
    if (opts.notes) {
      const notes = buildNotes(fn);
      if (notes.length) node.notes = notes;
    }
    const kids = (fn.children ?? []).filter((k) => k.visible !== false);
    const truncatedHere = kids.length === 0 && typeof fn.childCount === "number" && fn.childCount > 0;
    if (level >= depth || truncatedHere) {
      const remaining = truncatedHere ? fn.childCount : kids.length;
      if (remaining) node.more = remaining;
    } else {
      const childEls = [];
      let currentMaskEl;
      const childAncestors = [fn, ...ancestors].slice(0, 4);
      for (const child of kids) {
        const childEl = walk(child, level + 1, fn, path, fillForChildren, parent, childAncestors);
        if (!childEl) continue;
        childEls.push(childEl);
        if (child.isMask === true) {
          currentMaskEl = childEl;
        } else if (currentMaskEl) {
          const childNode = nodes[childEl];
          if (childNode) childNode.masked = currentMaskEl;
        }
      }
      if (childEls.length) node.children = childEls;
    }
    nodes[el] = node;
    if (node.children && node.children.length >= REPEAT_MIN) {
      compressRepeats(node, nodes);
    }
    return el;
  }
  const root = walk(file.document, 0);
  if (!root) {
    throw new Error("normalize: root node produced no output");
  }
  const tokens = interner.table();
  const estTokens = estimateTokens(JSON.stringify({ tokens, nodes }));
  const assetCount = Object.values(nodes).filter((n) => n.assetId).length;
  const doc = {
    file: { name: file.fileName, version: file.version },
    root,
    tokens,
    nodes,
    meta: {
      nodeCount: Object.keys(nodes).length,
      estTokens,
      depthUsed: depth
    },
    next: 'Read `tokens` for the design system, then build the UI from `nodes`. Tag each rendered element data-plumb-id="<el>". A node with a `more` count has that many children not yet included \u2014 call plumb_node again on that node\'s `id` to expand them.' + (assetCount > 0 ? ` This screen has ${assetCount} image asset(s) (nodes with \`assetId\`). Call plumb_assets with this screen's id to download them to disk BEFORE generating HTML \u2014 without the real files any <img> will be a placeholder and plumb_verify will flag every image node as asset.missing.` : "")
  };
  const suspicious = detectSuspiciousText(nodes);
  if (suspicious.length) doc.meta.suspiciousText = suspicious;
  if (opts.maxTokens && estTokens > opts.maxTokens) {
    doc.meta.truncated = true;
    doc.meta.hint = `Response is ~${estTokens} tokens, over the ${opts.maxTokens} budget. Re-call plumb_node with a smaller \`depth\`, or on a specific child \`el\`.`;
  }
  return doc;
}
var REPEAT_MIN = 3;
function fingerprintSubtree(el, nodes) {
  const n = nodes[el];
  if (!n) return "";
  const childPrints = (n.children ?? []).map((c) => fingerprintSubtree(c, nodes));
  const fillsForPrint = typeof n.fills === "string" ? n.fills : (n.fills ?? []).map(
    (f) => f.type === "image" ? { ...f, assetId: void 0 } : f
  );
  const skeleton = {
    type: n.type,
    box: n.box,
    layout: n.layout,
    fill: n.fill,
    fills: fillsForPrint.length > 0 ? fillsForPrint : void 0,
    inheritedFill: n.inheritedFill,
    stroke: n.stroke,
    strokeW: n.strokeW,
    strokeAlign: n.strokeAlign,
    strokeSides: n.strokeSides,
    strokeDash: n.strokeDash,
    radius: n.radius,
    radiusVar: n.radiusVar,
    shadow: n.shadow,
    effects: n.effects,
    backdropFilter: n.backdropFilter,
    opacity: n.opacity,
    clip: n.clip,
    text: n.text,
    textDecoration: n.textDecoration,
    component: n.component,
    pattern: n.pattern,
    // iconHint + notes are DERIVED from per-instance data (sibling chars,
    // etc.) so they legitimately vary across otherwise-identical rows.
    // Excluded here; iconHint surfaces as an override below.
    isMask: n.isMask,
    maskMode: n.maskMode,
    masked: n.masked,
    grow: n.grow,
    selfAlign: n.selfAlign,
    sizing: n.sizing,
    vectorPath: n.vectorPath,
    childPrints
  };
  return JSON.stringify(canonicalize(skeleton));
}
function canonicalize(v) {
  if (Array.isArray(v)) return v.map(canonicalize);
  if (v && typeof v === "object") {
    const out = {};
    for (const k of Object.keys(v).sort()) {
      const val = v[k];
      if (val === void 0) continue;
      out[k] = canonicalize(val);
    }
    return out;
  }
  return v;
}
function extractOverrides(templateEl, instanceEl, nodes) {
  const out = {};
  function walkPair(tEl, iEl) {
    const t = nodes[tEl];
    const i = nodes[iEl];
    if (!t || !i) return;
    const delta = {};
    if (t.chars !== i.chars && i.chars !== void 0) delta.chars = i.chars;
    if (t.assetId !== i.assetId && i.assetId !== void 0) delta.assetId = i.assetId;
    if (t.iconHint !== i.iconHint && i.iconHint !== void 0) delta.iconHint = i.iconHint;
    if (delta.chars !== void 0 || delta.assetId !== void 0 || delta.iconHint !== void 0) {
      out[tEl] = delta;
    }
    const tKids = t.children ?? [];
    const iKids = i.children ?? [];
    const n = Math.min(tKids.length, iKids.length);
    for (let k = 0; k < n; k++) walkPair(tKids[k], iKids[k]);
  }
  walkPair(templateEl, instanceEl);
  return out;
}
function deleteSubtree(el, nodes) {
  const n = nodes[el];
  if (!n) return;
  for (const c of n.children ?? []) deleteSubtree(c, nodes);
  delete nodes[el];
}
function compressRepeats(parent, nodes) {
  const kids = parent.children;
  if (!kids || kids.length < REPEAT_MIN) return;
  const prints = kids.map((k) => fingerprintSubtree(k, nodes));
  const groups = [];
  let i = 0;
  while (i < prints.length) {
    if (!prints[i] || prints[i].length === 0) {
      i++;
      continue;
    }
    let j = i + 1;
    while (j < prints.length && prints[j] === prints[i]) j++;
    const runLen = j - i;
    if (runLen >= REPEAT_MIN) {
      const templateEl = kids[i];
      const data = {};
      for (let k = i + 1; k < j; k++) {
        const instanceEl = kids[k];
        data[instanceEl] = extractOverrides(templateEl, instanceEl, nodes);
        deleteSubtree(instanceEl, nodes);
      }
      groups.push({ template: templateEl, data });
    }
    i = j;
  }
  if (groups.length === 0) return;
  parent.repeat = groups.length === 1 ? groups[0] : groups;
}
function normalizeComponentProps(raw) {
  if (!raw || typeof raw !== "object") return void 0;
  const out = {};
  for (const [key, entry] of Object.entries(raw)) {
    if (!entry || typeof entry !== "object") continue;
    const value = entry.value;
    if (typeof value !== "string" && typeof value !== "boolean" && typeof value !== "number") {
      continue;
    }
    const cleanKey = key.replace(/#[^#]*$/, "");
    out[cleanKey] = value;
  }
  return Object.keys(out).length > 0 ? out : void 0;
}
var VECTOR_INLINE_MAX = 600;
function inlineVectorPath(fn) {
  const geom = fn.fillGeometry;
  if (!Array.isArray(geom) || geom.length === 0) return void 0;
  const ds = [];
  for (const entry of geom) {
    if (!entry) continue;
    const d = typeof entry.path === "string" ? entry.path : entry.data;
    if (typeof d === "string" && d.length > 0) ds.push(d);
  }
  if (ds.length === 0) return void 0;
  const combined = ds.join(" ");
  if (combined.length > VECTOR_INLINE_MAX) return void 0;
  return combined;
}
function normalizeLayoutAlign(raw) {
  switch (raw) {
    case "STRETCH":
      return "stretch";
    case "MIN":
      return "min";
    case "CENTER":
      return "center";
    case "MAX":
      return "max";
    default:
      return void 0;
  }
}
function childSizing(fn) {
  const w = sizingValue(fn.layoutSizingHorizontal);
  const h = sizingValue(fn.layoutSizingVertical);
  if (!w && !h) return void 0;
  const out = {};
  if (w) out.w = w;
  if (h) out.h = h;
  return out;
}
function sizingValue(raw) {
  if (raw === "FILL") return "fill";
  if (raw === "HUG") return "hug";
  return void 0;
}
function normalizeStrokeAlign(raw) {
  switch (raw) {
    case "INSIDE":
      return "inside";
    case "OUTSIDE":
      return "outside";
    case "CENTER":
      return "center";
    default:
      return void 0;
  }
}
function perSideStrokeWidths(fn, uniform) {
  const isw = fn.individualStrokeWeights;
  const t = isw?.top ?? fn.strokeTopWeight;
  const r = isw?.right ?? fn.strokeRightWeight;
  const b = isw?.bottom ?? fn.strokeBottomWeight;
  const l = isw?.left ?? fn.strokeLeftWeight;
  if (t == null && r == null && b == null && l == null) return void 0;
  const tr = round(t ?? uniform ?? 0, 2);
  const rr = round(r ?? uniform ?? 0, 2);
  const br = round(b ?? uniform ?? 0, 2);
  const lr = round(l ?? uniform ?? 0, 2);
  if (tr === rr && rr === br && br === lr) return void 0;
  return { t: tr, r: rr, b: br, l: lr };
}
function normalizeMaskType(raw) {
  switch (raw) {
    case "ALPHA":
      return "alpha";
    case "LUMINANCE":
      return "luminance";
    case "VECTOR":
    case "GEOMETRY":
      return "vector";
    default:
      return void 0;
  }
}
function easingFromFigma(tr) {
  if (!tr) return void 0;
  const cubic = tr.easing?.easingFunctionCubicBezier;
  if (Array.isArray(cubic) && cubic.length === 4) {
    return `cubic-bezier(${cubic.map((n) => round(n, 3)).join(",")})`;
  }
  const kind = tr.easing?.type ?? tr.easingType;
  if (!kind) return void 0;
  switch (kind) {
    case "EASE_IN":
      return "ease-in";
    case "EASE_OUT":
      return "ease-out";
    case "EASE_IN_AND_OUT":
    case "EASE_IN_OUT":
      return "ease-in-out";
    case "LINEAR":
      return "linear";
    default:
      return kind.toLowerCase().replace(/_/g, "-");
  }
}
function motionFromReactions(reactions) {
  if (!reactions?.length) return void 0;
  const out = [];
  for (const r of reactions) {
    const trigger = r.trigger?.type;
    if (!trigger) continue;
    const action = r.action;
    if (!action) continue;
    const tr = action.transition;
    const kind = tr?.type ?? action.type ?? "INSTANT";
    const spec = { trigger, kind };
    if (typeof tr?.duration === "number") {
      const ms = tr.duration > 10 ? tr.duration : tr.duration * 1e3;
      spec.duration = round(ms, 0);
    }
    const easing = easingFromFigma(tr);
    if (easing) spec.easing = easing;
    if (action.destinationId) spec.target = action.destinationId;
    const overlayPos = action.overlayRelativePosition;
    const overlayBg = action.overlayBackground;
    if (overlayPos || overlayBg) {
      const overlay = {};
      if (overlayPos && typeof overlayPos.x === "number" && typeof overlayPos.y === "number") {
        overlay.pos = { x: round(overlayPos.x), y: round(overlayPos.y) };
      }
      if (overlayBg && overlayBg.type !== "NONE") {
        const c = overlayBg.color;
        if (c) {
          const channel2 = (n) => Math.round(n * 255).toString(16).padStart(2, "0");
          let hex = `#${channel2(c.r)}${channel2(c.g)}${channel2(c.b)}`;
          if (c.a !== void 0 && c.a < 1) hex += channel2(c.a);
          overlay.background = hex;
        }
      }
      if (overlay.pos || overlay.background) spec.overlay = overlay;
    }
    out.push(spec);
  }
  return out.length ? out : void 0;
}
var GENERIC_NAME = /^(image|vector|rectangle|rect|ellipse|circle|line|frame|group|instance|node|layer|shape|path|union|subtract|intersect|exclude|component)\s*\d*$/i;
var ICON_MAX_SIZE = 64;
function cleanIconLabel(s) {
  return s.replace(/([a-z])([A-Z])/g, "$1 $2").replace(/[_-]+/g, " ").replace(/\b(icon|button|btn|iconbutton|cta)\b/gi, "").replace(/\s{2,}/g, " ").trim();
}
function isDescriptive(name) {
  if (!name) return false;
  const cleaned = cleanIconLabel(name);
  if (!cleaned) return false;
  if (GENERIC_NAME.test(cleaned)) return false;
  return cleaned.length >= 2;
}
function hasImageFill(fn) {
  if (!Array.isArray(fn.fills)) return false;
  return fn.fills.some((p) => p?.visible !== false && p?.type === "IMAGE");
}
function isIconShaped(node, fn) {
  if (node.box.w > ICON_MAX_SIZE || node.box.h > ICON_MAX_SIZE) return false;
  if (node.box.w === 0 || node.box.h === 0) return false;
  const aspect = node.box.w / node.box.h;
  if (aspect < 0.5 || aspect > 2) return false;
  if (hasImageFill(fn)) return true;
  if (fn.type === "VECTOR" || fn.type === "BOOLEAN_OPERATION") return true;
  const name = (node.name ?? "").toLowerCase();
  if (name.includes("icon")) return true;
  return false;
}
function findTextSibling(parent, excludeId) {
  if (!parent?.children) return void 0;
  for (const sib of parent.children) {
    if (sib.id === excludeId) continue;
    if (sib.visible === false) continue;
    if (sib.type === "TEXT" && typeof sib.characters === "string") {
      const chars = sib.characters.trim();
      if (chars) return chars;
    }
  }
  return void 0;
}
function siblingLabel(fn, parent, grandparent) {
  const direct = findTextSibling(parent, fn.id);
  if (direct) return direct;
  if (grandparent && parent) {
    return findTextSibling(grandparent, parent.id);
  }
  return void 0;
}
function inferPattern(fn, node, layout) {
  if (!layout || layout.flow !== "row") return void 0;
  const { w, h } = node.box;
  if (w < 40 || w > 480 || h < 20 || h > 80) return void 0;
  const hasVisual = Boolean(node.stroke || node.fill || node.fills);
  if (!hasVisual) return void 0;
  if (node.radius === void 0) return void 0;
  const labeled = (fn.children ?? []).some(
    (c) => c.visible !== false && c.type === "TEXT" && typeof c.characters === "string" && c.characters.trim().length > 0
  );
  if (!labeled) return void 0;
  return "button";
}
function inferIconHint(fn, parent, grandparent, ancestors, node) {
  if (!isIconShaped(node, fn)) return void 0;
  if (isDescriptive(node.name ?? "")) {
    const own = cleanIconLabel(node.name ?? "");
    if (own) return own;
  }
  const label = siblingLabel(fn, parent, grandparent);
  if (label) return label;
  for (const anc of ancestors) {
    if (!isDescriptive(anc.name ?? "")) continue;
    const ancLabel = cleanIconLabel(anc.name ?? "");
    if (ancLabel) return ancLabel;
  }
  return void 0;
}
function relativePos(parent, child) {
  if (!parent) return void 0;
  const pbb = parent.absoluteBoundingBox;
  const cbb = child.absoluteBoundingBox;
  if (!pbb || !cbb) return void 0;
  const parentMode = parent.layoutMode;
  const autoLayoutParent = parentMode === "HORIZONTAL" || parentMode === "VERTICAL";
  const absoluteChild = child.layoutPositioning === "ABSOLUTE";
  if (autoLayoutParent && !absoluteChild) return void 0;
  return { x: round(cbb.x - pbb.x), y: round(cbb.y - pbb.y) };
}
function isFullRadius(r, box) {
  if (!Number.isFinite(r) || r <= 0) return false;
  const minSide = Math.min(box.w, box.h);
  if (minSide > 0 && r >= minSide / 2) return true;
  return r >= 9999;
}
function cornerRadius(fn, box) {
  if (fn.rectangleCornerRadii) {
    const [a, b, c, d] = fn.rectangleCornerRadii;
    if (a === b && b === c && c === d) {
      if (!a) return void 0;
      return isFullRadius(a, box) ? "full" : cleanPx(a);
    }
    const halfMin = Math.min(box.w, box.h) / 2;
    return fn.rectangleCornerRadii.map(
      (r) => cleanPx(isFullRadius(r, box) ? halfMin : r)
    );
  }
  if (typeof fn.cornerRadius === "number") {
    if (!fn.cornerRadius) return void 0;
    return isFullRadius(fn.cornerRadius, box) ? "full" : cleanPx(fn.cornerRadius);
  }
  return void 0;
}
function buildNotes(fn) {
  const notes = [];
  if (fn.layoutMode && fn.layoutMode !== "NONE") {
    notes.push(`auto-layout ${fn.layoutMode}`);
  }
  if (fn.layoutSizingHorizontal === "FIXED") notes.push("fixed width");
  if (fn.layoutSizingHorizontal === "FILL") notes.push("fills width");
  if (fn.layoutSizingHorizontal === "HUG") notes.push("hugs contents");
  return notes;
}
function cssBlendMode(figma) {
  switch (figma) {
    case "PASS_THROUGH":
    case "NORMAL":
      return void 0;
    case "DARKEN":
      return "darken";
    case "MULTIPLY":
      return "multiply";
    case "LINEAR_BURN":
      return "plus-darker";
    case "COLOR_BURN":
      return "color-burn";
    case "LIGHTEN":
      return "lighten";
    case "SCREEN":
      return "screen";
    case "LINEAR_DODGE":
      return "plus-lighter";
    case "COLOR_DODGE":
      return "color-dodge";
    case "OVERLAY":
      return "overlay";
    case "SOFT_LIGHT":
      return "soft-light";
    case "HARD_LIGHT":
      return "hard-light";
    case "DIFFERENCE":
      return "difference";
    case "EXCLUSION":
      return "exclusion";
    case "HUE":
      return "hue";
    case "SATURATION":
      return "saturation";
    case "COLOR":
      return "color";
    case "LUMINOSITY":
      return "luminosity";
    default:
      return figma.toLowerCase();
  }
}
function pdsTextGrow(figma) {
  switch (figma) {
    case "HEIGHT":
      return "h";
    case "WIDTH_AND_HEIGHT":
      return "wh";
    case "TRUNCATE":
      return "trunc";
    default:
      return void 0;
  }
}
function pdsConstraint(figma, axis) {
  if (!figma) return void 0;
  switch (figma) {
    case "MIN":
      return axis === "h" ? "left" : "top";
    case "MAX":
      return axis === "h" ? "right" : "bottom";
    case "CENTER":
      return "center";
    case "STRETCH":
      return "stretch";
    case "SCALE":
      return "scale";
    default:
      return void 0;
  }
}

// src/normalize/budget.ts
function normalizeToBudget(file, depth, maxTokens, opts = {}) {
  const precomputed = buildPreWalk(file);
  const baseOpts = { ...opts, precomputed };
  let pds = normalize2(file, depth, baseOpts);
  if (!maxTokens) return pds;
  let depthUsed = depth;
  while (pds.meta.estTokens > maxTokens && depthUsed > 0) {
    depthUsed -= 1;
    pds = normalize2(file, depthUsed, baseOpts);
  }
  if (pds.meta.estTokens > maxTokens) {
    pds = normalize2(file, depthUsed, { ...baseOpts, maxTokens });
  }
  return pds;
}

// src/render/captureFn.ts
var captureFn = function(selector) {
  const win = globalThis.window ?? globalThis;
  const doc = globalThis.document ?? win.document;
  const out = [];
  const els = doc.querySelectorAll(selector);
  els.forEach((el) => {
    const id = el.getAttribute("data-plumb-id");
    if (!id) return;
    const rect = el.getBoundingClientRect();
    const cs = win.getComputedStyle(el);
    const styles = {
      backgroundColor: cs.backgroundColor,
      color: cs.color,
      fontFamily: cs.fontFamily,
      fontSize: cs.fontSize,
      fontWeight: cs.fontWeight,
      lineHeight: cs.lineHeight,
      paddingTop: cs.paddingTop,
      paddingRight: cs.paddingRight,
      paddingBottom: cs.paddingBottom,
      paddingLeft: cs.paddingLeft,
      gap: cs.gap,
      flexDirection: cs.flexDirection,
      justifyContent: cs.justifyContent,
      alignItems: cs.alignItems,
      borderRadius: cs.borderRadius,
      borderColor: cs.borderColor,
      borderWidth: cs.borderWidth,
      opacity: cs.opacity,
      textDecorationLine: cs.textDecorationLine ?? cs.textDecoration ?? "",
      boxShadow: cs.boxShadow ?? "",
      // backdrop-filter is still vendor-prefixed in some engines (Safari/older
      // Chrome). Read the standard prop first, fall back to the -webkit- alias
      // so a real glass effect isn't reported as "(unset)".
      backdropFilter: cs.backdropFilter || cs.webkitBackdropFilter || ""
    };
    const tag = (el.tagName || "").toLowerCase();
    let img = tag === "img" || tag === "svg" || tag === "picture" || tag === "canvas";
    if (!img && cs.backgroundImage && cs.backgroundImage.indexOf("url(") !== -1) img = true;
    if (!img && typeof el.querySelector === "function") img = !!el.querySelector("img,svg,picture,canvas");
    const assetAttr = el.getAttribute("data-plumb-asset");
    out.push({
      el: id,
      box: { x: rect.x, y: rect.y, w: rect.width, h: rect.height },
      text: el.textContent ?? "",
      styles,
      asset: assetAttr || void 0,
      img: img || void 0
    });
  });
  return out;
};
function captureExpression(selector) {
  const sel = JSON.stringify(selector);
  return `(${captureFn.toString()})(${sel})`;
}

// src/render/capture.ts
async function captureRendered(b, pageUrl, selector, waitMs) {
  await navigate(b, pageUrl, waitMs);
  await evaluate(
    b,
    "new Promise(r => requestAnimationFrame(() => requestAnimationFrame(() => r(true))))"
  );
  const result = await evaluate(b, captureExpression(selector));
  return Array.isArray(result) ? result : [];
}

// src/normalize/resolve.ts
function resolveLayout(value, tokens) {
  if (value === void 0) return void 0;
  if (typeof value !== "string") return value;
  return tokens.layout?.[value];
}
function resolveFills(value, tokens) {
  if (value === void 0) return void 0;
  if (typeof value !== "string") return value;
  return tokens.fills?.[value];
}
function resolveEffects(value, tokens) {
  if (value === void 0) return void 0;
  if (typeof value !== "string") return value;
  return tokens.effects?.[value];
}

// src/verify.ts
var DEFAULT_TOLERANCES = {
  px: { ok: 1, warn: 3 },
  // v0.10 Phase 6 — colour distance is ΔE2000 (perceptually uniform). Thresholds:
  //   ≤ ok (1.0) → just-noticeable, never flag
  //   ≤ warn (3.5) → clearly different but plausibly within an agent's tolerance
  //   > warn → likely a real mismatch
  // Previously this was sum-of-abs-RGB-channel-deltas (ok=6, warn=24); the new
  // numbers are smaller because ΔE2000 is a different scale.
  color: { ok: 1, warn: 3.5 }
};
var MAX_DELTAS = 150;
function verifyAgainst(pds, rendered, tolerances = DEFAULT_TOLERANCES) {
  const byEl = /* @__PURE__ */ new Map();
  const byPath = /* @__PURE__ */ new Map();
  for (const el of Object.keys(pds.nodes)) {
    const node = pds.nodes[el];
    if (!node) continue;
    byEl.set(el, node);
    if (node.path) byPath.set(node.path, node);
  }
  const dupChars = collectDuplicateChars(pds);
  const ctx = { dupChars };
  const deltas = [];
  let matched = 0;
  let unmatched = 0;
  const matchedEls = /* @__PURE__ */ new Set();
  for (const r of rendered) {
    if (deltas.length >= MAX_DELTAS) break;
    const node = byEl.get(r.el) ?? byPath.get(r.el);
    if (!node) {
      unmatched += 1;
      deltas.push({
        el: r.el,
        name: r.el,
        kind: "missing-in-pds",
        expected: null,
        actual: r.el,
        severity: "warn"
      });
      continue;
    }
    matched += 1;
    matchedEls.add(node.el);
    compareOne(node, r, pds.tokens, tolerances, deltas, ctx);
  }
  const coverage = computeCoverage(pds, matchedEls);
  deltas.sort(
    (a, b) => severityRank(a.severity) - severityRank(b.severity) || (b.diff ?? 0) - (a.diff ?? 0)
  );
  const truncated = deltas.length > MAX_DELTAS;
  if (truncated) deltas.length = MAX_DELTAS;
  const ok2 = deltas.every((d) => d.severity !== "error");
  const base = {
    matched,
    rendered: rendered.length,
    unmatched,
    ok: ok2,
    deltas,
    coverage
  };
  if (truncated) base.truncated = true;
  return base;
}
function isImportantNode(node) {
  if (node.text || node.chars) return true;
  if (node.fill || node.fills) return true;
  if (node.effects || node.shadow || node.backdropFilter) return true;
  if (node.assetId) return true;
  if (node.radius !== void 0) return true;
  if (node.iconHint) return true;
  return false;
}
function computeCoverage(pds, matchedEls) {
  const reachable = /* @__PURE__ */ new Set();
  const queue = [pds.root];
  while (queue.length) {
    const el = queue.shift();
    if (!el || reachable.has(el)) continue;
    reachable.add(el);
    const node = pds.nodes[el];
    if (node?.children) queue.push(...node.children);
  }
  const importantUntagged = [];
  const plainUntagged = [];
  let importantTotal = 0;
  let importantMatched = 0;
  for (const el of reachable) {
    const node = pds.nodes[el];
    if (!node) continue;
    const important = isImportantNode(node);
    if (important) importantTotal += 1;
    if (matchedEls.has(el)) {
      if (important) importantMatched += 1;
      continue;
    }
    if (important) importantUntagged.push(el);
    else plainUntagged.push(el);
  }
  const untagged = importantUntagged.concat(plainUntagged).slice(0, 20);
  const pdsTotal = reachable.size;
  const coverage = pdsTotal === 0 ? 1 : matchedEls.size / pdsTotal;
  return {
    pdsTotal,
    matched: matchedEls.size,
    coverage: Math.round(coverage * 100) / 100,
    untagged,
    importantTotal,
    importantMatched
  };
}
function compareOne(node, r, tokens, tol, deltas, ctx) {
  const styles = r.styles ?? {};
  const pushPx = (kind, expected, actual, custom) => {
    const diff = Math.abs(expected - actual);
    const t = custom ?? tol.px;
    if (diff <= t.ok) return;
    const severity = diff > t.warn ? "error" : "warn";
    deltas.push({ el: node.el, name: node.name, kind, expected, actual, diff, severity });
  };
  const wEstimate = node.sizing?.w === "fill" || node.sizing?.w === "hug" || node.type === "text" && node.textGrow === "wh";
  const hEstimate = node.sizing?.h === "fill" || node.sizing?.h === "hug" || node.type === "text" && (node.textGrow === "h" || node.textGrow === "wh");
  if (node.box.w > 0 && r.box.w > 0 && !wEstimate) pushPx("size.w", node.box.w, r.box.w);
  if (node.box.h > 0 && r.box.h > 0 && !hEstimate) pushPx("size.h", node.box.h, r.box.h);
  const layout = resolveLayout(node.layout, tokens);
  if (layout) {
    const pdsFlow = layout.flow === "col" ? "column" : "row";
    const renFlow = styles.flexDirection;
    if (renFlow && renFlow !== pdsFlow) {
      deltas.push({
        el: node.el,
        name: node.name,
        kind: "layout.flow",
        expected: pdsFlow,
        actual: renFlow,
        severity: "error"
      });
    }
    if (layout.gap !== void 0) {
      const v = parsePx(styles.gap);
      if (v !== null) pushPx("layout.gap", layout.gap, v);
    }
    const pad = layout.pad;
    const sideMap = [
      ["pad.top", pad[0], parsePx(styles.paddingTop)],
      ["pad.right", pad[1], parsePx(styles.paddingRight)],
      ["pad.bottom", pad[2], parsePx(styles.paddingBottom)],
      ["pad.left", pad[3], parsePx(styles.paddingLeft)]
    ];
    for (const [kind, expected, actual] of sideMap) {
      if (actual !== null) pushPx(kind, expected, actual);
    }
    if (layout.justify) {
      const v = styles.justifyContent;
      if (v && v !== layout.justify) {
        deltas.push({
          el: node.el,
          name: node.name,
          kind: "layout.justify",
          expected: layout.justify,
          actual: v,
          severity: "warn"
        });
      }
    }
    if (layout.align) {
      const v = styles.alignItems;
      if (v && v !== layout.align) {
        deltas.push({
          el: node.el,
          name: node.name,
          kind: "layout.align",
          expected: layout.align,
          actual: v,
          severity: "warn"
        });
      }
    }
  }
  if (node.fill && node.fill.startsWith("$c") && node.type !== "text" && styles.backgroundColor) {
    pushColorDelta(
      node,
      "fill",
      tokens.color[node.fill],
      styles.backgroundColor,
      tol,
      deltas
    );
  }
  if (styles.backgroundColor && isUserAgentColor(styles.backgroundColor)) {
    deltas.push({
      el: node.el,
      name: node.name,
      kind: "ua-style-fallthrough",
      expected: "explicit background-color",
      actual: styles.backgroundColor,
      severity: "warn"
    });
  }
  if (typeof node.chars === "string" && typeof r.text === "string") {
    const exp = node.chars.trim();
    const act = r.text.trim();
    if (exp !== act) {
      const placeholder = isPlaceholderText(exp, ctx.dupChars.has(exp));
      deltas.push({
        el: node.el,
        name: node.name,
        kind: placeholder ? "text.placeholder" : "text.chars",
        expected: exp,
        actual: act,
        severity: placeholder ? "info" : "warn"
      });
    }
  }
  if (node.type === "text" && node.fill && node.fill.startsWith("$c") && styles.color) {
    pushColorDelta(node, "text.color", tokens.color[node.fill], styles.color, tol, deltas);
  }
  if (node.type === "text" && node.textDecoration) {
    const dec = (styles.textDecorationLine ?? styles.textDecoration ?? "").toLowerCase();
    if (!dec.includes(node.textDecoration)) {
      deltas.push({
        el: node.el,
        name: node.name,
        kind: "text.decoration",
        expected: node.textDecoration,
        actual: dec || "none",
        severity: "error"
      });
    }
  }
  if (node.text && node.text.startsWith("$t")) {
    const tk = tokens.text[node.text];
    const parsed = tk ? parseTextToken(tk) : null;
    if (parsed) {
      const renSize = parsePx(styles.fontSize);
      if (renSize !== null) pushPx("text.size", parsed.size, renSize, { ok: 0.5, warn: 1.5 });
      const renWeight = normalizeWeight(styles.fontWeight);
      if (renWeight !== null && renWeight !== parsed.weight) {
        const diff = Math.abs(renWeight - parsed.weight);
        deltas.push({
          el: node.el,
          name: node.name,
          kind: "text.weight",
          expected: parsed.weight,
          actual: renWeight,
          diff,
          severity: diff <= 100 ? "warn" : "error"
        });
      }
      if (parsed.lh) {
        const renRatio = computeLineHeightRatio(styles.lineHeight, renSize);
        if (renRatio !== null) {
          const diff = Math.abs(renRatio - parsed.lh);
          if (diff > 0.05) {
            deltas.push({
              el: node.el,
              name: node.name,
              kind: "text.lh",
              expected: parsed.lh,
              actual: Math.round(renRatio * 100) / 100,
              diff,
              severity: diff > 0.15 ? "error" : "warn"
            });
          }
        }
      }
      if (parsed.family && styles.fontFamily) {
        const ff = styles.fontFamily.toLowerCase();
        if (!ff.includes(parsed.family.toLowerCase())) {
          deltas.push({
            el: node.el,
            name: node.name,
            kind: "text.family",
            expected: parsed.family,
            actual: styles.fontFamily,
            severity: "warn"
          });
        }
      }
    }
  }
  if (node.radius !== void 0 && styles.borderRadius) {
    let expected = null;
    if (typeof node.radius === "string") expected = tokens.radius[node.radius] ?? null;
    else if (Array.isArray(node.radius)) expected = node.radius[0] ?? null;
    if (expected !== null) {
      const v = parsePx(styles.borderRadius);
      if (v !== null) {
        if (expected === "full") {
          const minSide = Math.min(node.box.w, node.box.h);
          if (minSide > 0 && v + tol.px.ok < minSide / 2) {
            deltas.push({
              el: node.el,
              name: node.name,
              kind: "radius",
              expected: `full (>= ${Math.round(minSide / 2 * 100) / 100}px)`,
              actual: v,
              severity: v + tol.px.warn < minSide / 2 ? "error" : "warn"
            });
          }
        } else {
          pushPx("radius", expected, v);
        }
      }
    }
  }
  if (node.stroke && node.stroke.startsWith("$c") && styles.borderColor) {
    pushColorDelta(node, "stroke", tokens.color[node.stroke], styles.borderColor, tol, deltas);
  }
  if (node.strokeW !== void 0 && styles.borderWidth) {
    const v = parsePx(styles.borderWidth);
    if (v !== null) pushPx("stroke.width", node.strokeW, v);
  }
  if (typeof node.opacity === "number" && styles.opacity) {
    const v = parseFloat(styles.opacity);
    if (!Number.isNaN(v)) {
      const diff = Math.abs(node.opacity - v);
      if (diff > 0.05) {
        deltas.push({
          el: node.el,
          name: node.name,
          kind: "opacity",
          expected: node.opacity,
          actual: v,
          diff,
          severity: diff > 0.15 ? "error" : "warn"
        });
      }
    }
  }
  const expectedShadow = typeof node.shadow === "string" && node.shadow.startsWith("$s") ? tokens.shadow[node.shadow] : node.shadow;
  if (expectedShadow && (!styles.boxShadow || styles.boxShadow === "none")) {
    deltas.push({
      el: node.el,
      name: node.name,
      kind: "shadow.missing",
      expected: expectedShadow,
      actual: styles.boxShadow ?? "(unset)",
      severity: "error"
    });
  } else if (expectedShadow && styles.boxShadow) {
    const expBlur = parseShadowBlur(expectedShadow);
    const renBlur = parseShadowBlur(styles.boxShadow);
    if (expBlur !== null && renBlur !== null) {
      const diff = Math.abs(expBlur - renBlur);
      if (diff > tol.px.warn) {
        deltas.push({
          el: node.el,
          name: node.name,
          kind: "shadow.blur",
          expected: expBlur,
          actual: renBlur,
          diff,
          severity: "warn"
        });
      }
    }
  }
  if (node.backdropFilter) {
    const ren = (styles.backdropFilter ?? "").trim();
    if (!ren || ren.toLowerCase() === "none") {
      deltas.push({
        el: node.el,
        name: node.name,
        kind: "backdrop.missing",
        expected: node.backdropFilter,
        actual: ren || "(unset)",
        severity: "error"
      });
    } else {
      const expBlur = parseBlurRadius(node.backdropFilter);
      const renBlur = parseBlurRadius(ren);
      if (expBlur !== null && renBlur !== null) {
        const diff = Math.abs(expBlur - renBlur);
        if (diff > tol.px.warn) {
          deltas.push({
            el: node.el,
            name: node.name,
            kind: "backdrop.blur",
            expected: expBlur,
            actual: renBlur,
            diff,
            severity: "warn"
          });
        }
      }
    }
  }
  if (typeof node.rotation === "number" && Math.abs(node.rotation) > 0.5) {
    const renderedDeg = parseRotation(styles.transform);
    if (renderedDeg !== null) {
      const diff = Math.abs(node.rotation - renderedDeg);
      if (diff > 1) {
        deltas.push({
          el: node.el,
          name: node.name,
          kind: "rotation",
          expected: round2(node.rotation, 2),
          actual: round2(renderedDeg, 2),
          diff,
          severity: diff > 5 ? "error" : "warn"
        });
      }
    }
  }
  if (typeof node.grow === "number" && node.grow > 0 && styles.flexGrow) {
    const v = parseFloat(styles.flexGrow);
    if (!Number.isNaN(v) && Math.abs(v - node.grow) > 0.01) {
      deltas.push({
        el: node.el,
        name: node.name,
        kind: "flex.grow",
        expected: node.grow,
        actual: v,
        diff: Math.abs(v - node.grow),
        severity: "warn"
      });
    }
  }
  if (node.selfAlign && styles.alignSelf && styles.alignSelf !== "auto") {
    const cssAlign = node.selfAlign === "stretch" ? "stretch" : node.selfAlign === "min" ? "flex-start" : node.selfAlign === "max" ? "flex-end" : node.selfAlign === "center" ? "center" : void 0;
    if (cssAlign && cssAlign !== styles.alignSelf) {
      deltas.push({
        el: node.el,
        name: node.name,
        kind: "flex.selfAlign",
        expected: cssAlign,
        actual: styles.alignSelf,
        severity: "warn"
      });
    }
  }
  const fillsValue = typeof node.fills === "string" ? tokens.fills?.[node.fills] : node.fills;
  if (Array.isArray(fillsValue) && fillsValue.length > 1) {
    const bg = styles.backgroundImage ?? "";
    const layers = bg && bg !== "none" ? bg.split(/,\s*(?![^()]*\))/).length : 0;
    if (layers < fillsValue.length) {
      deltas.push({
        el: node.el,
        name: node.name,
        kind: "fills.count",
        expected: fillsValue.length,
        actual: layers,
        diff: fillsValue.length - layers,
        severity: "warn"
      });
    }
  }
  const isRaster = typeof node.assetId === "string" && node.assetId.length > 0;
  const isVectorAsset = node.type === "vector" || node.type === "image" || node.vectorPath !== void 0;
  if (isRaster || isVectorAsset) {
    const rAsset = r.asset;
    if (isRaster && rAsset && rAsset !== node.assetId) {
      deltas.push({
        el: node.el,
        name: node.name,
        kind: "asset.mismatch",
        expected: node.assetId ?? null,
        actual: rAsset,
        severity: "error"
      });
    } else if (isRaster && rAsset === node.assetId) {
    } else if (!r.img) {
      deltas.push({
        el: node.el,
        name: node.name,
        kind: "asset.missing",
        expected: isRaster ? node.assetId ?? "exported asset" : "image/vector content",
        actual: "(none \u2014 redrawn or omitted)",
        severity: "error"
      });
    } else if (isRaster && !rAsset) {
      deltas.push({
        el: node.el,
        name: node.name,
        kind: "asset.untagged",
        expected: node.assetId ?? null,
        actual: "(image present, data-plumb-asset missing)",
        severity: "warn"
      });
    }
  }
}
function parseRotation(transform) {
  if (!transform || transform === "none") return null;
  const rot = transform.match(/rotate(?:Z)?\(\s*(-?\d+(?:\.\d+)?)deg\s*\)/);
  if (rot && rot[1]) return Number(rot[1]);
  const m2 = transform.match(/^matrix\(\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)/);
  if (m2 && m2[1] && m2[2]) {
    const a = Number(m2[1]);
    const b = Number(m2[2]);
    return Math.atan2(b, a) * 180 / Math.PI;
  }
  return null;
}
function round2(n, places) {
  const p = Math.pow(10, places);
  return Math.round(n * p) / p;
}
function parseBlurRadius(s) {
  if (!s) return null;
  const m = s.match(/blur\(\s*(-?\d+(?:\.\d+)?)px\s*\)/i);
  return m && m[1] ? parseFloat(m[1]) : null;
}
function parseShadowBlur(s) {
  if (!s || s === "none") return null;
  const first = s.split(/,(?![^()]*\))/)[0] ?? s;
  const stripped = first.replace(/(?:rgba?|hsla?)\([^)]*\)/gi, " ").replace(/#[0-9a-f]+/gi, " ").replace(/\binset\b/gi, " ");
  const nums = stripped.match(/-?\d+(?:\.\d+)?px/gi);
  if (!nums || nums.length < 3) return null;
  return parseFloat(nums[2]);
}
function collectDuplicateChars(pds) {
  const counts = /* @__PURE__ */ new Map();
  for (const el of Object.keys(pds.nodes)) {
    const node = pds.nodes[el];
    if (!node || typeof node.chars !== "string") continue;
    const t = node.chars.trim();
    if (!t) continue;
    counts.set(t, (counts.get(t) ?? 0) + 1);
  }
  const dup = /* @__PURE__ */ new Set();
  for (const [s, n] of counts) if (n >= 3) dup.add(s);
  return dup;
}
var PLACEHOLDER_LABELS = /* @__PURE__ */ new Set([
  "title",
  "subtitle",
  "heading",
  "subheading",
  "header",
  "body",
  "body text",
  "text",
  "label",
  "caption",
  "description",
  "placeholder",
  "content",
  "lorem ipsum",
  "name",
  "first name",
  "last name",
  "full name",
  "your name",
  "email",
  "email address",
  "username",
  "company name",
  "address"
]);
function isPlaceholderText(s, isDuplicate) {
  if (isDuplicate) return true;
  const t = s.trim();
  if (t === "") return true;
  const low = t.toLowerCase();
  if (PLACEHOLDER_LABELS.has(low)) return true;
  if (/lorem ipsum|\bdummy\b|\bplaceholder\b|sample text/i.test(t)) return true;
  if (t.length > 60) return true;
  if (/^(.)\1{2,}$/.test(t)) return true;
  if (/\d/.test(t) && /^[\s$€£₹%.,:+\-–—()0-9/]+$/.test(t)) return true;
  return false;
}
function pushColorDelta(node, kind, expectedHex, renderedColor, tol, deltas) {
  if (!expectedHex) return;
  const exp = parseColor(expectedHex);
  const actual = parseColor(renderedColor);
  if (!exp || !actual) return;
  const diff = colorDistance(exp, actual);
  if (diff <= tol.color.ok) return;
  deltas.push({
    el: node.el,
    name: node.name,
    kind,
    expected: expectedHex,
    actual: rgbToHex(actual),
    diff,
    severity: diff > tol.color.warn ? "error" : "warn"
  });
}
function parseColor(s) {
  if (!s) return null;
  const t = s.trim().toLowerCase();
  if (t === "" || t === "transparent" || t === "none" || t === "currentcolor") return null;
  const hexMatch = t.match(/^#([0-9a-f]+)$/);
  if (hexMatch && hexMatch[1]) {
    let hex = hexMatch[1];
    if (hex.length === 3 || hex.length === 4) {
      hex = hex.split("").map((c) => c + c).join("");
    }
    if (hex.length === 6 || hex.length === 8) {
      const r = parseInt(hex.slice(0, 2), 16);
      const g = parseInt(hex.slice(2, 4), 16);
      const b = parseInt(hex.slice(4, 6), 16);
      const a = hex.length === 8 ? parseInt(hex.slice(6, 8), 16) / 255 : 1;
      if (![r, g, b].some(Number.isNaN)) return { r, g, b, a };
    }
    return null;
  }
  const rgbMatch = t.match(/^rgba?\s*\(([^)]+)\)$/);
  if (rgbMatch && rgbMatch[1]) {
    const parts = rgbMatch[1].split(/[\s,/]+/).filter(Boolean);
    if (parts.length >= 3) {
      const r = Math.round(parseFloat(parts[0]));
      const g = Math.round(parseFloat(parts[1]));
      const b = Math.round(parseFloat(parts[2]));
      const a = parts[3] !== void 0 ? parseFloat(parts[3]) : 1;
      if (![r, g, b].some(Number.isNaN)) {
        return { r, g, b, a: Number.isNaN(a) ? 1 : a };
      }
    }
  }
  return null;
}
function colorDistance(a, b) {
  const la = rgbaToLab(a);
  const lb = rgbaToLab(b);
  return deltaE2000(la, lb);
}
function srgbToLinear(c) {
  const x = c / 255;
  return x <= 0.04045 ? x / 12.92 : Math.pow((x + 0.055) / 1.055, 2.4);
}
function rgbaToLab({ r, g, b }) {
  const R = srgbToLinear(r);
  const G = srgbToLinear(g);
  const B = srgbToLinear(b);
  const X = R * 0.4124564 + G * 0.3575761 + B * 0.1804375;
  const Y = R * 0.2126729 + G * 0.7151522 + B * 0.072175;
  const Z = R * 0.0193339 + G * 0.119192 + B * 0.9503041;
  const Xn = 0.95047;
  const Yn = 1;
  const Zn = 1.08883;
  const f = (t) => t > 216 / 24389 ? Math.cbrt(t) : 24389 / 27 * t / 116 + 16 / 116;
  const fx = f(X / Xn);
  const fy = f(Y / Yn);
  const fz = f(Z / Zn);
  return { l: 116 * fy - 16, a: 500 * (fx - fy), b: 200 * (fy - fz) };
}
function deltaE2000(la, lb) {
  const deg = (rad2) => rad2 * 180 / Math.PI;
  const rad = (d) => d * Math.PI / 180;
  const Lbar = (la.l + lb.l) / 2;
  const C1 = Math.hypot(la.a, la.b);
  const C2 = Math.hypot(lb.a, lb.b);
  const Cbar = (C1 + C2) / 2;
  const G = 0.5 * (1 - Math.sqrt(Math.pow(Cbar, 7) / (Math.pow(Cbar, 7) + Math.pow(25, 7))));
  const a1 = la.a * (1 + G);
  const a2 = lb.a * (1 + G);
  const C1p = Math.hypot(a1, la.b);
  const C2p = Math.hypot(a2, lb.b);
  const Cpbar = (C1p + C2p) / 2;
  const h1 = Math.atan2(la.b, a1);
  const h2 = Math.atan2(lb.b, a2);
  const h1d = (deg(h1) % 360 + 360) % 360;
  const h2d = (deg(h2) % 360 + 360) % 360;
  let dhp = h2d - h1d;
  if (Math.abs(dhp) > 180) dhp += dhp > 0 ? -360 : 360;
  const dLp = lb.l - la.l;
  const dCp = C2p - C1p;
  const dHp = 2 * Math.sqrt(C1p * C2p) * Math.sin(rad(dhp / 2));
  let Hpbar = h1d + h2d;
  if (Math.abs(h1d - h2d) > 180) Hpbar += Hpbar < 360 ? 360 : -360;
  Hpbar /= 2;
  const T = 1 - 0.17 * Math.cos(rad(Hpbar - 30)) + 0.24 * Math.cos(rad(2 * Hpbar)) + 0.32 * Math.cos(rad(3 * Hpbar + 6)) - 0.2 * Math.cos(rad(4 * Hpbar - 63));
  const SL = 1 + 0.015 * Math.pow(Lbar - 50, 2) / Math.sqrt(20 + Math.pow(Lbar - 50, 2));
  const SC = 1 + 0.045 * Cpbar;
  const SH = 1 + 0.015 * Cpbar * T;
  const dTheta = 30 * Math.exp(-Math.pow((Hpbar - 275) / 25, 2));
  const RC = 2 * Math.sqrt(Math.pow(Cpbar, 7) / (Math.pow(Cpbar, 7) + Math.pow(25, 7)));
  const RT = -RC * Math.sin(2 * rad(dTheta));
  return Math.sqrt(
    Math.pow(dLp / SL, 2) + Math.pow(dCp / SC, 2) + Math.pow(dHp / SH, 2) + RT * (dCp / SC) * (dHp / SH)
  );
}
function rgbToHex(c) {
  const ch = (n) => Math.max(0, Math.min(255, Math.round(n))).toString(16).padStart(2, "0");
  const base = `#${ch(c.r)}${ch(c.g)}${ch(c.b)}`;
  return c.a >= 1 ? base : base + ch(c.a * 255);
}
function parsePx(s) {
  if (s === void 0 || s === null) return null;
  const t = s.trim();
  if (t === "0") return 0;
  if (t === "" || t === "auto" || t === "normal") return null;
  const m = t.match(/^(-?\d+(?:\.\d+)?)\s*px$/i);
  if (m && m[1]) return parseFloat(m[1]);
  const n = parseFloat(t);
  return Number.isFinite(n) ? n : null;
}
function parseTextToken(s) {
  const m = s.trim().match(
    /^(\d+)\s+(\d+(?:\.\d+)?)px(?:\s*\/\s*(\d+(?:\.\d+)?))?\s*(.*)$/
  );
  if (!m || !m[1] || !m[2]) return null;
  const family = m[4] && m[4].trim() ? m[4].trim() : void 0;
  return {
    weight: parseInt(m[1], 10),
    size: parseFloat(m[2]),
    lh: m[3] ? parseFloat(m[3]) : void 0,
    family
  };
}
function normalizeWeight(s) {
  if (!s) return null;
  if (/^\d+$/.test(s)) return parseInt(s, 10);
  const map = {
    normal: 400,
    bold: 700,
    lighter: 300,
    bolder: 800
  };
  return map[s.toLowerCase()] ?? null;
}
function computeLineHeightRatio(lh, size) {
  if (!lh) return null;
  const lhPx = parsePx(lh);
  if (lhPx !== null && size !== null && size > 0) return lhPx / size;
  const n = parseFloat(lh);
  if (Number.isFinite(n) && !lh.includes("px") && !lh.includes("%")) return n;
  return null;
}
function severityRank(s) {
  return s === "error" ? 0 : s === "warn" ? 1 : 2;
}
var UA_COLOR_KEYWORDS = /* @__PURE__ */ new Set([
  "buttonface",
  "buttontext",
  "field",
  "fieldtext",
  "highlight",
  "highlighttext",
  "graytext",
  "menu",
  "menutext",
  "window",
  "windowframe",
  "windowtext",
  "linktext",
  "visitedtext",
  "activetext",
  "activeborder",
  "inactiveborder",
  "infobackground",
  "infotext",
  "scrollbar",
  "threeddarkshadow",
  "threedface",
  "threedhighlight",
  "threedlightshadow",
  "threedshadow"
]);
function isUserAgentColor(s) {
  if (!s) return false;
  return UA_COLOR_KEYWORDS.has(s.trim().toLowerCase());
}

// src/cli/fit.ts
var HELP = `plumb-mcp fit \u2014 generate a pixel-matching build and self-correct it against Figma

Usage:
  plumb-mcp fit <figma-url>
  plumb-mcp fit --node <id> --file <fileKey>

Arguments:
  <figma-url>               A Figma URL \u2014 fileKey + node are auto-extracted.

Options:
  --node <id>               Figma node id (with --file) instead of a URL.
  --file <fileKey>          Figma file key.
  --model <id>              Generator model. Default ${DEFAULT_FIT_MODEL}. Try an Opus id for max fidelity.
  --iters <n>               Max correction passes. Default 5.
  --accept <0-100>          Stop once the score clears this. Default ${DEFAULT_ACCEPT}.
  --out <path>              Where to write the final HTML. Default ./plumb-fit.html
  --depth <n>               PDS depth. Default 12.
  --wait <ms>               Settle time after each render. Default 1200.
  --chrome <path>           Explicit Chrome binary path.
  --json                    Print the machine-readable run summary.
  --verbose                 Tee Chrome stderr (debug aid).
  -h, --help                Show this message.

Environment:
  FIGMA_TOKEN               Read-only Figma token (figma.com \u2192 Settings \u2192 Security).
  ANTHROPIC_API_KEY         Key for the generator model.
  PLUMB_FIT_MODEL           Default model override.
  PLUMB_CHROME / CHROME_PATH  Chrome binary override.

Example:
  plumb-mcp fit https://www.figma.com/design/abc/Flow?node-id=190-109884
`;
function parseArgs(argv) {
  const out = {
    model: process.env.PLUMB_FIT_MODEL ?? DEFAULT_FIT_MODEL,
    iters: 5,
    accept: DEFAULT_ACCEPT,
    out: "plumb-fit.html",
    waitMs: 1200,
    depth: 12,
    verbose: false,
    json: false,
    showHelp: false
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "-h" || a === "--help") out.showHelp = true;
    else if (a === "--verbose") out.verbose = true;
    else if (a === "--json") out.json = true;
    else if (a === "--node") out.node = argv[++i];
    else if (a === "--file") out.fileKey = argv[++i];
    else if (a === "--model") out.model = argv[++i] ?? out.model;
    else if (a === "--out") out.out = argv[++i] ?? out.out;
    else if (a === "--iters") out.iters = Number(argv[++i] ?? out.iters);
    else if (a === "--accept") out.accept = Number(argv[++i] ?? out.accept);
    else if (a === "--depth") out.depth = Number(argv[++i] ?? out.depth);
    else if (a === "--wait") out.waitMs = Number(argv[++i] ?? out.waitMs);
    else if (a === "--chrome") out.chrome = argv[++i];
    else if (a.startsWith("-"))
      throw new Error(`Unknown flag "${a}". Run \`plumb-mcp fit --help\` for usage.`);
    else if (!out.url) out.url = a;
    else throw new Error(`Unexpected positional argument "${a}".`);
  }
  if (out.url && !out.figmaUrl) out.figmaUrl = out.url;
  return out;
}
function colorize(s, code) {
  if (!process.stdout.isTTY) return s;
  return `\x1B[${code}m${s}\x1B[0m`;
}
function bar10(score) {
  const filled = Math.round(Math.max(0, Math.min(100, score)) / 100 * 10);
  return "\u25B0".repeat(filled) + "\u25B1".repeat(10 - filled);
}
async function runFitCli(argv) {
  let args;
  try {
    args = parseArgs(argv);
  } catch (e) {
    process.stderr.write(`plumb-mcp fit: ${e.message}
`);
    return 2;
  }
  if (args.showHelp) {
    process.stdout.write(HELP);
    return 0;
  }
  let fileKey;
  let nodeId2;
  try {
    const t = resolveFigmaTarget({ url: args.figmaUrl, fileKey: args.fileKey, id: args.node });
    fileKey = t.fileKey;
    nodeId2 = t.id;
  } catch (e) {
    process.stderr.write(`plumb-mcp fit: ${e.message}
`);
    return 2;
  }
  if (!fileKey || !nodeId2) {
    process.stderr.write(
      "plumb-mcp fit needs a Figma node \u2014 pass a Figma URL, or --node <id> with --file <fileKey>.\n"
    );
    return 2;
  }
  const figmaToken = process.env.FIGMA_TOKEN ?? process.env.FIGMA_ACCESS_TOKEN;
  if (!figmaToken) {
    process.stderr.write(
      "plumb-mcp fit: FIGMA_TOKEN is not set (figma.com \u2192 Settings \u2192 Security \u2192 personal access tokens).\n"
    );
    return 4;
  }
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    process.stderr.write(
      "plumb-mcp fit: ANTHROPIC_API_KEY is not set \u2014 needed to generate the build. Export it and retry.\n"
    );
    return 4;
  }
  const chromePath = findChrome(args.chrome);
  if (!chromePath || !existsSync3(chromePath)) {
    process.stderr.write((chromePath ? `Chrome path "${chromePath}" does not exist.` : chromeInstallHint()) + "\n");
    return 3;
  }
  process.stderr.write(`[plumb fit] Fetching PDS for ${fileKey}:${nodeId2}\u2026
`);
  const file = await fetchNodeViaRest({ fileKey, nodeId: nodeId2, depth: args.depth + 1, token: figmaToken });
  const pds = normalizeToBudget(file, args.depth, void 0, { notes: false });
  const screen = file.document.name ?? nodeId2;
  const browser = await launchBrowser({ chromePath, verbose: args.verbose });
  const passes = [];
  let prevHtml;
  let deltas;
  let finalHtml = "";
  let finalScore = 0;
  let done = false;
  try {
    if (!args.json) {
      process.stderr.write(`[plumb fit] ${screen} \u2014 up to ${args.iters} pass(es), accept \u2265 ${args.accept}%

`);
    }
    for (let i = 1; i <= args.iters; i++) {
      const html = await generateHtml({ pds, prevHtml, deltas, model: args.model, apiKey });
      finalHtml = html;
      const served = await serveHtml(html);
      let rendered;
      try {
        rendered = await captureRendered(browser, served.url, "[data-plumb-id]", args.waitMs);
      } finally {
        await served.close();
      }
      const result = verifyAgainst(pds, rendered, DEFAULT_TOLERANCES);
      const fit = buildFitResponse(result, { accept: args.accept, iteration: i });
      finalScore = fit.score;
      done = fit.done;
      passes.push({
        iteration: i,
        score: fit.score,
        done: fit.done,
        errors: fit.errors,
        warns: fit.warns,
        matched: fit.matched,
        importantTotal: fit.importantTotal
      });
      if (!args.json) {
        const pct = `${fit.score.toFixed(1)}%`.padStart(6);
        const status = fit.done ? colorize("\u2713", "32") : " ";
        process.stderr.write(
          `  iter ${i}  ${pct}  ${bar10(fit.score)}  ${status}  ${fit.errors} err \xB7 ${fit.warns} warn \xB7 ${fit.importantMatched}/${fit.importantTotal} key nodes
`
        );
      }
      prevHtml = html;
      deltas = result.deltas;
      if (fit.done) break;
    }
  } finally {
    await browser.close();
  }
  const outPath = resolvePath(process.cwd(), args.out);
  writeFileSync(outPath, finalHtml, "utf8");
  if (args.json) {
    process.stdout.write(
      JSON.stringify({ screen, fileKey, nodeId: nodeId2, accept: args.accept, model: args.model, done, finalScore, out: outPath, passes }, null, 2) + "\n"
    );
  } else {
    const verdict = done ? colorize(`Pixel-perfect \u2014 ${finalScore.toFixed(1)}%`, "32") : colorize(`Stopped at ${finalScore.toFixed(1)}% after ${passes.length} pass(es)`, "33");
    process.stderr.write(`
[plumb fit] ${verdict}
`);
    process.stderr.write(`[plumb fit] Wrote ${outPath}
`);
    if (!done) {
      process.stderr.write(`[plumb fit] Raise --iters or inspect the file; the design may have nodes the generator can't infer (assets, custom fonts).
`);
    }
  }
  return done ? 0 : 1;
}

// src/cli/init.ts
import { existsSync as existsSync4, mkdirSync, readFileSync as readFileSync3, writeFileSync as writeFileSync2 } from "fs";
import { homedir } from "os";
import { dirname as dirname3, join as join6 } from "path";
import { fileURLToPath as fileURLToPath4 } from "url";
function targets(cwd) {
  const home = homedir();
  return [
    {
      name: "Claude Code",
      detect: [join6(home, ".claude"), join6(home, ".claude.json")],
      configFile: join6(cwd, ".mcp.json"),
      key: "mcpServers"
    },
    {
      name: "Cursor",
      detect: [join6(home, ".cursor")],
      configFile: join6(cwd, ".cursor", "mcp.json"),
      key: "mcpServers"
    },
    {
      name: "VS Code",
      detect: [
        join6(home, ".vscode"),
        join6(home, "Library", "Application Support", "Code")
      ],
      configFile: join6(cwd, ".vscode", "mcp.json"),
      key: "servers"
    },
    {
      name: "Windsurf",
      detect: [join6(home, ".codeium", "windsurf")],
      configFile: join6(home, ".codeium", "windsurf", "mcp_config.json"),
      key: "mcpServers"
    }
  ];
}
function mergeConfig(file, key, entry) {
  let config = {};
  if (existsSync4(file)) {
    try {
      config = JSON.parse(readFileSync3(file, "utf8"));
    } catch {
      config = {};
    }
  }
  const servers = config[key] ?? {};
  servers.plumb = entry;
  config[key] = servers;
  mkdirSync(dirname3(file), { recursive: true });
  writeFileSync2(file, JSON.stringify(config, null, 2) + "\n");
}
function runInit() {
  const cwd = process.cwd();
  const entry = fileURLToPath4(import.meta.url);
  const pkgRoot = dirname3(dirname3(entry));
  const token = process.env.FIGMA_TOKEN ?? "figd_your_read_only_token";
  const serverEntry = { command: "node", args: [entry], env: { FIGMA_TOKEN: token } };
  const out = ["", "Plumb \xB7 init", "\u2500".repeat(58)];
  let wrote = 0;
  for (const t of targets(cwd)) {
    if (t.detect.some((p) => existsSync4(p))) {
      mergeConfig(t.configFile, t.key, serverEntry);
      out.push(`  \u2713 ${t.name.padEnd(13)}\u2192 ${t.configFile}`);
      wrote += 1;
    } else {
      out.push(`  \xB7 ${t.name.padEnd(13)}  not detected (skipped)`);
    }
  }
  out.push("\u2500".repeat(58));
  if (wrote === 0) {
    out.push("No supported editor detected. Add this to your MCP client config:");
    out.push(JSON.stringify({ mcpServers: { plumb: serverEntry } }, null, 2));
  }
  if (!process.env.FIGMA_TOKEN) {
    out.push(
      "",
      "\u26A0 FIGMA_TOKEN was written as a placeholder. The REST tools need a real",
      "  read-only token (figma.com \u2192 Settings \u2192 Security). The plugin path",
      "  needs no token \u2014 see below."
    );
  }
  out.push(
    "",
    "Figma plugin (plugin path \u2014 no token, no rate limits):",
    "  Figma desktop \u2192 Plugins \u2192 Development \u2192 Import plugin from manifest",
    `  \u2192 ${join6(pkgRoot, "figma-plugin", "manifest.json")}`,
    "",
    "Restart your editor to load the Plumb MCP server.",
    ""
  );
  process.stdout.write(out.join("\n"));
}

// src/cli/studio.ts
import { spawn as spawn2 } from "child_process";
var HELP2 = `plumb-mcp studio \u2014 open the live Plumb Studio cockpit

Usage:
  plumb-mcp studio            Find the running Plumb server and open Studio in your browser.
  plumb-mcp studio --print    Just print the URL (don't open a browser).

Studio mirrors what your AI agent is doing \u2014 the design, the live build, the
climbing match score, and the activity timeline \u2014 and can also drive Plumb's
own self-healing loop with in-UI approvals.
`;
async function findRunningPort() {
  for (const port of BRIDGE_PORTS) {
    try {
      const ctrl = new AbortController();
      const timer = setTimeout(() => ctrl.abort(), 500);
      await fetch(`http://127.0.0.1:${port}/`, { signal: ctrl.signal });
      clearTimeout(timer);
      return port;
    } catch {
    }
  }
  return null;
}
function openBrowser(url) {
  const platform = process.platform;
  const [cmd, args] = platform === "darwin" ? ["open", [url]] : platform === "win32" ? ["cmd", ["/c", "start", "", url]] : ["xdg-open", [url]];
  try {
    spawn2(cmd, args, { stdio: "ignore", detached: true }).unref();
  } catch {
  }
}
async function runStudioCli(argv) {
  if (argv.includes("-h") || argv.includes("--help")) {
    process.stdout.write(HELP2);
    return 0;
  }
  const port = await findRunningPort();
  if (!port) {
    process.stderr.write(
      `plumb-mcp studio: no running Plumb server found on 127.0.0.1 (ports ${BRIDGE_PORTS[0]}\u2013${BRIDGE_PORTS[BRIDGE_PORTS.length - 1]}).
Start your MCP client (Claude Code / Cursor / Windsurf) so it launches plumb-mcp, then re-run this.
`
    );
    return 1;
  }
  const url = `http://127.0.0.1:${port}/`;
  process.stdout.write(`Plumb Studio \u2192 ${url}
`);
  if (!argv.includes("--print")) openBrowser(url);
  return 0;
}

// src/cli/verify.ts
import { existsSync as existsSync5, readFileSync as readFileSync4 } from "fs";
var HELP3 = `plumb-mcp verify \u2014 diff your rendered UI against the Figma design

Usage:
  plumb-mcp verify <url> --node <figma-node-id>
  plumb-mcp verify <url> --url <figma-url>

Arguments:
  <url>                     Your running dev server URL (e.g. http://localhost:5173/dashboard)

Required (one of):
  --node <id>               Figma node id (e.g. 190:109884)
  --url <figma-url>         Paste a Figma URL \u2014 fileKey + node are auto-extracted
  --name <name>             Screen name (with --file)

Common options:
  --file <fileKey>          Figma file key (defaults from --url or env)
  --depth <n>               PDS depth (default 12 \u2014 deep enough for most screens)
  --selector <css>          Capture elements matching this selector
                            (default "[data-plumb-id]")
  --wait <ms>               Wait this long after page load before capturing (default 1500)
  --chrome <path>           Explicit Chrome binary path
  --json                    Print machine-readable JSON instead of the table
  --verbose                 Tee Chrome stderr to ours (debug aid)
  -h, --help                Show this message

Environment:
  FIGMA_TOKEN               Required \u2014 same token plumb-mcp uses for REST.
  PLUMB_CHROME / CHROME_PATH  Override Chrome binary path.

Example:
  plumb-mcp verify http://localhost:5173/dashboard --node 190:109884
`;
function parseArgs2(argv) {
  const out = {
    selector: "[data-plumb-id]",
    waitMs: 1500,
    depth: 12,
    verbose: false,
    json: false,
    showHelp: false
  };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === "-h" || a === "--help") out.showHelp = true;
    else if (a === "--verbose") out.verbose = true;
    else if (a === "--json") out.json = true;
    else if (a === "--node") out.node = argv[++i];
    else if (a === "--name") out.name = argv[++i];
    else if (a === "--file") out.fileKey = argv[++i];
    else if (a === "--url") out.figmaUrl = argv[++i];
    else if (a === "--chrome") out.chrome = argv[++i];
    else if (a === "--selector") out.selector = argv[++i] ?? out.selector;
    else if (a === "--wait") out.waitMs = Number(argv[++i] ?? 1500);
    else if (a === "--depth") out.depth = Number(argv[++i] ?? 12);
    else if (a.startsWith("-")) throw new Error(`Unknown flag "${a}". Run \`plumb-mcp verify --help\` for usage.`);
    else if (!out.url) out.url = a;
    else throw new Error(`Unexpected positional argument "${a}".`);
  }
  return out;
}
function requireToken() {
  const t = process.env.FIGMA_TOKEN ?? process.env.FIGMA_ACCESS_TOKEN;
  if (!t) {
    throw new Error(
      "FIGMA_TOKEN is not set. Create a read-only token at figma.com \u2192 Settings \u2192 Security \u2192 personal access tokens, then export it."
    );
  }
  return t;
}
function colorize2(s, code) {
  if (!process.stdout.isTTY) return s;
  return `\x1B[${code}m${s}\x1B[0m`;
}
function severityIcon(sev) {
  if (sev === "error") return colorize2("\u2717", "31");
  if (sev === "warn") return colorize2("!", "33");
  return colorize2("\xB7", "90");
}
function printTable(result, pageUrl, screen) {
  const cov = result.coverage;
  const pct = cov ? Math.round(cov.coverage * 100) : 100;
  const matchedLine = `${result.matched}/${cov?.pdsTotal ?? result.matched}`;
  const status = result.ok ? colorize2("OK", "32") : colorize2("FAIL", "31");
  process.stdout.write(`
plumb verify \xB7 ${status}
`);
  process.stdout.write(`  page    ${pageUrl}
`);
  if (screen) process.stdout.write(`  screen  ${screen}
`);
  process.stdout.write(`  matched ${matchedLine} (${pct}% coverage)
`);
  process.stdout.write(`  deltas  ${result.deltas.length} (${countBy(result.deltas, "error")} errors, ${countBy(result.deltas, "warn")} warns)
`);
  if (result.deltas.length) {
    process.stdout.write("\n");
    for (const d of result.deltas.slice(0, 60)) {
      process.stdout.write(
        `  ${severityIcon(d.severity)} ${d.el} \xB7 ${d.kind} \u2014 expected ${fmtVal2(d.expected)}, actual ${fmtVal2(d.actual)}
`
      );
    }
    if (result.deltas.length > 60) {
      process.stdout.write(`  \u2026 and ${result.deltas.length - 60} more (re-run with --json for the full list)
`);
    }
  }
  if (cov && cov.untagged.length) {
    process.stdout.write("\nUntagged but visible in the PDS subtree \u2014 consider tagging next round:\n");
    process.stdout.write("  " + cov.untagged.slice(0, 12).join(", "));
    if (cov.untagged.length > 12) process.stdout.write(`, \u2026 +${cov.untagged.length - 12}`);
    process.stdout.write("\n");
  }
  process.stdout.write("\n");
}
function countBy(deltas, sev) {
  return deltas.filter((d) => d.severity === sev).length;
}
function fmtVal2(v) {
  if (v === null) return "\u2014";
  return typeof v === "string" ? `"${v}"` : String(v);
}
function readSelfVersion() {
  try {
    const pkg2 = JSON.parse(readFileSync4(new URL("../../package.json", import.meta.url), "utf8"));
    return pkg2.version ?? "0.0.0";
  } catch {
    return "0.0.0";
  }
}
async function runVerifyCli(argv) {
  let args;
  try {
    args = parseArgs2(argv);
  } catch (e) {
    process.stderr.write(`plumb-mcp verify: ${e.message}
`);
    return 2;
  }
  if (args.showHelp) {
    process.stdout.write(HELP3);
    return 0;
  }
  if (!args.url) {
    process.stderr.write("plumb-mcp verify: missing <url>. Run `plumb-mcp verify --help`.\n");
    return 2;
  }
  let fileKey;
  let nodeId2;
  try {
    const t = resolveFigmaTarget({
      url: args.figmaUrl,
      fileKey: args.fileKey,
      id: args.node
    });
    fileKey = t.fileKey;
    nodeId2 = t.id;
  } catch (e) {
    process.stderr.write(`plumb-mcp verify: ${e.message}
`);
    return 2;
  }
  if (!fileKey || !nodeId2) {
    process.stderr.write(
      "plumb-mcp verify needs a Figma node \u2014 pass --node <id> (with --file), --url <figma-url>, or set FIGMA_FILE_KEY.\n"
    );
    return 2;
  }
  const chromePath = findChrome(args.chrome);
  if (!chromePath) {
    process.stderr.write(chromeInstallHint() + "\n");
    return 3;
  }
  if (!existsSync5(chromePath)) {
    process.stderr.write(`Chrome path "${chromePath}" does not exist.
`);
    return 3;
  }
  let token;
  try {
    token = requireToken();
  } catch (e) {
    process.stderr.write(`plumb-mcp verify: ${e.message}
`);
    return 4;
  }
  process.stderr.write(`[1/3] Fetching PDS for ${fileKey}:${nodeId2}\u2026
`);
  const file = await fetchNodeViaRest({
    fileKey,
    nodeId: nodeId2,
    depth: args.depth + 1,
    token
  });
  const pds = normalizeToBudget(file, args.depth, void 0, { notes: false });
  process.stderr.write(`[2/3] Launching ${chromePath.split("/").pop()}\u2026
`);
  const browser = await launchBrowser({ chromePath, verbose: args.verbose });
  let rendered;
  try {
    process.stderr.write(`[3/3] Capturing ${args.selector} on ${args.url}\u2026
`);
    rendered = await captureRendered(browser, args.url, args.selector, args.waitMs);
  } finally {
    await browser.close();
  }
  if (rendered.length === 0) {
    process.stderr.write(
      `
No elements matched ${args.selector} on ${args.url}. Tag your rendered elements with data-plumb-id="<el>" or pass --selector to override.
`
    );
    return 5;
  }
  const result = verifyAgainst(pds, rendered, DEFAULT_TOLERANCES);
  if (args.json) {
    process.stdout.write(JSON.stringify(result, null, 2) + "\n");
  } else {
    printTable(result, args.url, file.document.name ?? null);
  }
  return result.ok ? 0 : 1;
}
var VERIFY_CLI_VERSION = readSelfVersion();

// src/server.ts
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

// src/tools/assets.ts
import { z } from "zod";

// src/bridge/inventory.ts
function formatScreenMatches(matches) {
  return matches.map((m) => ({
    id: m.id,
    name: m.name,
    page: m.page,
    box: { w: m.w, h: m.h }
  }));
}
function flatScreens() {
  const out = [];
  if (!bridge.inventory) return out;
  for (const page of bridge.inventory.pages) {
    for (const f of page.frames) {
      out.push({ id: f.id, name: f.name, page: page.name, w: f.w, h: f.h });
    }
  }
  return out;
}
function screenName(id) {
  return flatScreens().find((s) => s.id === id)?.name ?? "";
}
function resolveScreen(id, name) {
  if (id) return { id };
  if (!name) {
    throw new PlumbError(
      "Provide a screen `id` or `name`.",
      "Call plumb_outline to list the available screen names and ids."
    );
  }
  const q = name.trim().toLowerCase();
  const screens = flatScreens();
  let matches = screens.filter((s) => s.name.toLowerCase() === q);
  if (matches.length === 0) {
    matches = screens.filter((s) => s.name.toLowerCase().includes(q));
  }
  if (matches.length === 0) {
    throw new PlumbError(
      `No screen matches "${name}".`,
      "Call plumb_outline to see the available screen names."
    );
  }
  if (matches.length === 1) return { id: matches[0].id };
  return { ambiguous: matches };
}
function pluginOutline() {
  const inv = bridge.inventory;
  if (!inv) {
    throw new PlumbError(
      "The Plumb plugin is paired, but it has not sent the file inventory yet.",
      "Wait a moment and retry; if it persists, re-run the plugin in Figma."
    );
  }
  const minter = new HandleMinter();
  const pages = inv.pages.map((page) => ({
    name: page.name,
    screens: page.frames.map((f) => ({
      id: f.id,
      el: minter.mint(f.name, "frame"),
      name: f.name,
      box: { w: f.w, h: f.h }
    }))
  }));
  const screenCount = pages.reduce((n, p) => n + p.screens.length, 0);
  const outline = {
    source: "plugin",
    file: { name: inv.fileName },
    pages,
    meta: { pageCount: pages.length, screenCount, estTokens: 0 },
    next: "Call plumb_node with a screen `id` (or `name`) to extract it, and plumb_assets with the same id to export its icons and images."
  };
  outline.meta.estTokens = estimateTokens(JSON.stringify(outline));
  return outline;
}

// src/assets.ts
import {
  mkdirSync as mkdirSync2,
  renameSync,
  statSync as statSync2,
  copyFileSync,
  unlinkSync,
  readFileSync as readFileSync5
} from "fs";
import { join as join7 } from "path";
var INLINE_MAX_BYTES = 64 * 1024;
var MIME_FOR_FORMAT = {
  SVG: "image/svg+xml",
  PNG: "image/png",
  JPG: "image/jpeg",
  GIF: "image/gif",
  WEBP: "image/webp"
};
function buildInlineSource(format, path, bytes) {
  if (format === "SVG") {
    try {
      return readFileSync5(path, "utf8");
    } catch {
      return void 0;
    }
  }
  if (bytes > INLINE_MAX_BYTES) return void 0;
  const mime = MIME_FOR_FORMAT[format];
  if (!mime) return void 0;
  try {
    const buf = readFileSync5(path);
    return `data:${mime};base64,${buf.toString("base64")}`;
  } catch {
    return void 0;
  }
}
function extForFormat(format) {
  switch (format) {
    case "SVG":
      return "svg";
    case "JPG":
      return "jpg";
    case "GIF":
      return "gif";
    case "WEBP":
      return "webp";
    default:
      return "png";
  }
}
function slug2(s) {
  return s.toLowerCase().normalize("NFKD").replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 48) || "";
}
function writeAssets(screenName2, assets, opts = {}) {
  const root = process.env.PLUMB_ASSETS_DIR ?? join7(process.cwd(), "plumb-assets");
  const dir = join7(root, slug2(screenName2) || "screen");
  mkdirSync2(dir, { recursive: true });
  const used = /* @__PURE__ */ new Set();
  const written = [];
  for (const asset of assets) {
    if (!asset.path) continue;
    const ext = extForFormat(asset.format);
    const base = slug2(asset.name) || "asset";
    let file = `${base}.${ext}`;
    let n = 2;
    while (used.has(file)) file = `${base}-${n++}.${ext}`;
    used.add(file);
    const fullPath = join7(dir, file);
    try {
      renameSync(asset.path, fullPath);
    } catch {
      copyFileSync(asset.path, fullPath);
      try {
        unlinkSync(asset.path);
      } catch {
      }
    }
    const bytes = statSync2(fullPath).size;
    const out = {
      id: asset.id,
      name: asset.name,
      format: asset.format,
      path: fullPath,
      bytes,
      parentId: asset.parentId
    };
    if (opts.inline) {
      const source = buildInlineSource(asset.format, fullPath, bytes);
      if (source !== void 0) out.source = source;
    }
    written.push(out);
  }
  return { dir, written };
}

// src/tools/shared.ts
function ok(payload) {
  return { content: [{ type: "text", text: JSON.stringify(payload) }] };
}
function fail(e) {
  return {
    content: [{ type: "text", text: JSON.stringify(toErrorPayload(e)) }],
    isError: true
  };
}
function requireToken2() {
  const token = process.env.FIGMA_TOKEN ?? process.env.FIGMA_ACCESS_TOKEN;
  if (!token) {
    throw new PlumbError(
      "No Figma token configured.",
      "Set FIGMA_TOKEN in the MCP server's env block. Create a read-only token at figma.com \u2192 Settings \u2192 Security \u2192 personal access tokens."
    );
  }
  return token;
}

// src/tools/assets.ts
var DESCRIPTION = "Export Figma assets \u2014 icons as SVG, images as PNG \u2014 through the paired plugin. Three modes:\n  \u2022 Default \u2014 `id` or `name` of a screen \u2192 recursive export of every asset in it, written to a local folder.\n  \u2022 List \u2014 same + `list: true` \u2192 just the manifest (id, name, format, parentId) of available assets; no files written. Cheap; use first to see what's there.\n  \u2022 Surgical \u2014 `ids: [...]` \u2192 export exactly those node ids (one file each, no recursion). Preferred once you know what you need.";
function registerPlumbAssets(server) {
  server.registerTool(
    "plumb_assets",
    {
      title: "Plumb \xB7 assets",
      description: DESCRIPTION,
      inputSchema: {
        id: z.string().optional().describe("Screen/node id to scope the export to."),
        name: z.string().optional().describe("Screen name \u2014 resolved against the file."),
        ids: z.array(z.string()).optional().describe(
          "Surgical \u2014 export exactly these node ids (one each, no recursion)."
        ),
        list: z.boolean().optional().describe(
          "Manifest only \u2014 return id/name/format/parentId per candidate; no file writes."
        ),
        raw: z.boolean().optional().describe(
          "For nodes with IMAGE fills, export the original uploaded bytes (JPG/PNG/GIF/WEBP) via getImageByHash, instead of a 2\xD7 rasterised PNG render. Other nodes (icons, vectors) export the same as default."
        ),
        inline: z.boolean().optional().describe(
          "Return asset bytes inline on each row: SVGs as raw markup, bitmaps under 64KB as a data: URI. Useful for dropping SVG icons straight into JSX without re-reading from disk. Files are still written; this is additive."
        )
      },
      annotations: { readOnlyHint: false, idempotentHint: true, openWorldHint: false }
    },
    async (args) => {
      try {
        if (!bridge.paired) {
          throw new PlumbError(
            "No Figma plugin is paired.",
            "plumb_assets uses the plugin path \u2014 run the Plumb plugin in Figma and click 'Pair with Plumb'."
          );
        }
        const list = args.list === true;
        const raw = args.raw === true;
        const inline = args.inline === true;
        if (args.ids && args.ids.length > 0) {
          const { assets: assets2, error: error2 } = await requestAssets({ ids: args.ids, list, raw });
          if (error2) {
            throw new PlumbError(
              `The plugin could not export the requested ids: ${error2}`,
              "Retry; if it persists, the ids may be invalid or deleted."
            );
          }
          if (list) {
            return ok({
              source: "plugin",
              mode: "list",
              count: assets2.length,
              manifest: assets2.map((a) => ({
                id: a.id,
                name: a.name,
                format: a.format,
                parentId: a.parentId
              })),
              next: assets2.length === 0 ? "None of the supplied ids resolved to exportable nodes." : "Pick what you need, then call plumb_assets with `ids: [...]` and no `list`."
            });
          }
          const folder = args.name || (args.id ? screenName(args.id) : "") || "specific";
          const { dir: dir2, written: written2 } = writeAssets(folder, assets2, { inline });
          return ok({
            source: "plugin",
            mode: "specific",
            dir: dir2,
            count: written2.length,
            assets: written2.map((w) => ({
              id: w.id,
              name: w.name,
              format: w.format,
              path: w.path,
              bytes: w.bytes,
              parentId: w.parentId,
              ...w.source !== void 0 ? { source: w.source } : {}
            })),
            next: written2.length === 0 ? "None of the supplied ids resolved to exportable nodes." : inline ? "Each row has either `source` (SVG markup or data: URI) or just `path`. Drop SVG `source` straight into JSX." : "Use these file paths when building."
          });
        }
        if (!args.id && !args.name) {
          throw new PlumbError(
            "Provide a screen `id` or `name` \u2014 or `ids: [...]` for surgical export.",
            "Call plumb_outline to list screen names and ids."
          );
        }
        const resolved = resolveScreen(args.id, args.name);
        if ("ambiguous" in resolved) {
          return ok({
            ambiguous: true,
            matches: formatScreenMatches(resolved.ambiguous),
            next: "Several screens share that name. Each row shows `page` and `box` (w\xD7h) \u2014 pick the one you want and re-call plumb_assets with its `id`."
          });
        }
        const { assets, error } = await requestAssets({ nodeId: resolved.id, list, raw });
        if (error) {
          throw new PlumbError(
            `The plugin could not export assets: ${error}`,
            "Retry; if it persists, the screen may have been deleted or renamed."
          );
        }
        const sname = screenName(resolved.id) || resolved.id;
        if (list) {
          return ok({
            source: "plugin",
            mode: "list",
            screen: sname,
            count: assets.length,
            manifest: assets.map((a) => ({
              id: a.id,
              name: a.name,
              format: a.format,
              parentId: a.parentId
            })),
            next: assets.length === 0 ? "No exportable assets in this screen." : "Pick the ones you actually need, then call plumb_assets with `ids: [...]` (no `list`)."
          });
        }
        const { dir, written } = writeAssets(sname, assets, { inline });
        return ok({
          source: "plugin",
          dir,
          count: written.length,
          assets: written.map((w) => ({
            id: w.id,
            name: w.name,
            format: w.format,
            path: w.path,
            bytes: w.bytes,
            parentId: w.parentId,
            ...w.source !== void 0 ? { source: w.source } : {}
          })),
          next: written.length === 0 ? "No exportable assets (icons / images) were found in this screen." : inline ? "Each row has either `source` (SVG markup or data: URI) or just `path`. Drop SVG `source` straight into JSX." : "Use these file paths when building. Tip: pass `list: true` first to peek without downloading; then `ids: [...]` to pull just what you need."
        });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/brand.ts
import { z as z2 } from "zod";

// src/dsl/layout.ts
var JUSTIFY2 = {
  start: "flex-start",
  center: "center",
  end: "flex-end",
  between: "space-between"
};
var ALIGN2 = {
  start: "flex-start",
  center: "center",
  end: "flex-end",
  stretch: "stretch",
  baseline: "baseline"
};
var SELF = {
  start: "min",
  center: "center",
  end: "max",
  stretch: "stretch"
};
function mapJustify(j) {
  if (j === void 0) return void 0;
  const v = JUSTIFY2[j];
  return v === "flex-start" ? void 0 : v;
}
function mapAlign(a) {
  if (a === void 0) return void 0;
  const v = ALIGN2[a];
  return v === "flex-start" ? void 0 : v;
}
function mapSelf(s) {
  return s === void 0 ? void 0 : SELF[s];
}
function resolveWidth(w, avail) {
  if (w === void 0 || w === "hug") return void 0;
  if (w === "fill") return avail;
  if (typeof w === "number") return w;
  const pct = parsePercent(w);
  if (pct !== void 0) return round(avail * pct / 100, 2);
  return void 0;
}
function resolveHeight(h, avail) {
  if (h === void 0 || h === "hug" || h === "fill") return void 0;
  if (typeof h === "number") return h;
  const pct = parsePercent(h);
  if (pct !== void 0 && avail !== void 0) return round(avail * pct / 100, 2);
  return void 0;
}
function parsePercent(s) {
  const m = /^(\d+(?:\.\d+)?)%$/.exec(s.trim());
  return m ? Number(m[1]) : void 0;
}
function sizingFor(w, h) {
  const out = {};
  if (w === "fill") out.w = "fill";
  else if (w === "hug") out.w = "hug";
  else if (typeof w === "number") out.w = "fixed";
  if (h === "fill") out.h = "fill";
  else if (h === "hug") out.h = "hug";
  else if (typeof h === "number") out.h = "fixed";
  return out.w || out.h ? out : void 0;
}
function measureContainer(flow, gap, pad, children) {
  const [t, r, b, l] = pad;
  const n = children.length;
  const gaps = n > 1 ? gap * (n - 1) : 0;
  if (flow === "row") {
    const inner2 = children.reduce((s, c) => s + c.w, 0) + gaps;
    const cross2 = children.reduce((m, c) => Math.max(m, c.h), 0);
    return { w: round(inner2 + l + r, 2), h: round(cross2 + t + b, 2) };
  }
  const inner = children.reduce((s, c) => s + c.h, 0) + gaps;
  const cross = children.reduce((m, c) => Math.max(m, c.w), 0);
  return { w: round(cross + l + r, 2), h: round(inner + t + b, 2) };
}
function measureText(chars, rts, maxWidth, measurer) {
  if (measurer) {
    const m = measurer.measure(chars, rts, maxWidth);
    return { w: round(m.w, 2), h: round(m.h, 2) };
  }
  const lineH = rts.size * rts.line;
  const avg = rts.size * 0.52 + rts.tracking;
  const oneLine = Math.max(chars.length, 1) * avg;
  if (maxWidth !== void 0 && oneLine > maxWidth) {
    const lines = Math.max(1, Math.ceil(oneLine / maxWidth));
    return { w: round(maxWidth, 2), h: round(lines * lineH, 2) };
  }
  return { w: round(oneLine, 2), h: round(lineH, 2) };
}
function buildLayout(flow, opts) {
  const layout = { flow, pad: opts.pad };
  if (opts.gap) layout.gap = opts.gap;
  if (opts.wrap) {
    layout.wrap = true;
    if (opts.gapCross) layout.gapCross = opts.gapCross;
  }
  const j = mapJustify(opts.justify);
  if (j) layout.justify = j;
  const a = mapAlign(opts.align);
  if (a) layout.align = a;
  return layout;
}

// src/dsl/motion.ts
var TRIGGER_MAP = {
  click: "ON_CLICK",
  hover: "ON_HOVER",
  press: "ON_PRESS",
  drag: "ON_DRAG",
  "mouse-enter": "MOUSE_ENTER",
  "mouse-leave": "MOUSE_LEAVE",
  "mouse-down": "MOUSE_DOWN",
  "mouse-up": "MOUSE_UP"
};
var TRANSITION_KIND_MAP = {
  smart: "SMART_ANIMATE",
  dissolve: "DISSOLVE",
  "move-in": "MOVE_IN",
  "move-out": "MOVE_OUT",
  push: "PUSH",
  "slide-in": "SLIDE_IN",
  "slide-out": "SLIDE_OUT",
  "scroll-animate": "SCROLL_ANIMATE",
  instant: "INSTANT"
};
var KEY_CODES = {
  enter: 13,
  return: 13,
  esc: 27,
  escape: 27,
  space: 32,
  spacebar: 32,
  tab: 9,
  backspace: 8,
  delete: 46,
  up: 38,
  down: 40,
  left: 37,
  right: 39,
  home: 36,
  end: 35
};
function parseKeys(name) {
  const key = name.trim().toLowerCase();
  if (!key) return [];
  if (key in KEY_CODES) return [KEY_CODES[key]];
  if (/^\d+$/.test(key)) return [parseInt(key, 10)];
  const first = key[0];
  if (first) return [first.toUpperCase().charCodeAt(0)];
  return [];
}
function parseTrigger(on) {
  if (on.startsWith("key:")) {
    return { trigger: "ON_KEY_DOWN", keys: parseKeys(on.slice(4)) };
  }
  if (on.startsWith("after:")) {
    const ms = parseInt(on.slice(6), 10);
    return { trigger: "AFTER_TIMEOUT", timeout: Number.isFinite(ms) ? ms : 0 };
  }
  return { trigger: TRIGGER_MAP[on] ?? "ON_CLICK" };
}
function dslEasingToMotion(e) {
  if (!e) return {};
  if (typeof e === "string") return { easing: e };
  if ("bezier" in e) {
    const [a, b, c, d] = e.bezier;
    return { easing: `cubic-bezier(${a},${b},${c},${d})` };
  }
  if ("spring" in e) return { easing: "custom-spring", spring: e.spring };
  return {};
}
function actionOf(it, ctx) {
  const resolve = (ref) => ctx.resolveTarget?.(ref) ?? ref;
  if (it.go !== void 0) return { navigation: "NAVIGATE", target: resolve(it.go) };
  if (it.swap !== void 0) return { navigation: "SWAP", target: resolve(it.swap) };
  if (it.overlay !== void 0) return { navigation: "OVERLAY", target: resolve(it.overlay) };
  if (it.scrollTo !== void 0) return { navigation: "SCROLL_TO", target: resolve(it.scrollTo) };
  if (it.back) return { navigation: "BACK" };
  if (it.close) return { navigation: "CLOSE" };
  if (it.url !== void 0) return { navigation: "URL", url: it.url };
  if (it.set !== void 0) return { navigation: "SET_VAR", setVars: it.set };
  return null;
}
function compileInteraction(it, ctx = {}) {
  const action = actionOf(it, ctx);
  if (!action) {
    ctx.warn?.(`interaction on "${it.on}" has no action verb; skipped`);
    return null;
  }
  const { trigger, timeout, keys } = parseTrigger(it.on);
  const spec = {
    trigger,
    kind: "INSTANT",
    navigation: action.navigation
  };
  if (action.target !== void 0) spec.target = action.target;
  if (action.url !== void 0) spec.url = action.url;
  if (action.setVars !== void 0) spec.setVars = action.setVars;
  if (timeout !== void 0) spec.timeout = timeout;
  if (keys !== void 0) spec.keys = keys;
  if (it.animate) {
    const t = it.animate;
    spec.kind = TRANSITION_KIND_MAP[t.kind] ?? "INSTANT";
    if (t.from) spec.direction = t.from.toUpperCase();
    spec.duration = t.duration ?? 300;
    const { easing, spring } = dslEasingToMotion(t.ease);
    if (easing) spec.easing = easing;
    if (spring) spec.spring = spring;
    if (t.matchLayers !== void 0) spec.matchLayers = t.matchLayers;
  }
  if (it.keepScroll) spec.preserveScroll = true;
  if (it.resetState) spec.resetState = true;
  return spec;
}
function compileInteractions(interactions, ctx = {}) {
  if (!interactions?.length) return [];
  const out = [];
  for (const it of interactions) {
    const spec = compileInteraction(it, ctx);
    if (spec) out.push(spec);
  }
  return out;
}

// src/dsl/components.ts
function indexComponents(components) {
  const map = /* @__PURE__ */ new Map();
  for (const c of components ?? []) map.set(c.name, c);
  return map;
}
function hasSlotBlock(block) {
  if (block.type === "slot") return true;
  if (block.type === "stack") return block.children.some(hasSlotBlock);
  return false;
}
function buildComponentMaster(comp) {
  const props = {};
  for (const def of comp.props ?? []) {
    if (def.default !== void 0) props[def.name] = def.default;
  }
  return cloneBlock(comp.body, props, {});
}
function expandInstance(inst, components) {
  const comp = components.get(inst.component);
  if (!comp) return null;
  const props = {};
  for (const def of comp.props ?? []) {
    if (def.default !== void 0) props[def.name] = def.default;
  }
  for (const [k, v] of Object.entries(inst.props ?? {})) props[k] = v;
  const block = cloneBlock(comp.body, props, inst.slots ?? {});
  return { block, componentName: comp.name };
}
function cloneBlock(block, props, slots) {
  const rec = { ...block };
  for (const field of STRING_FIELDS) {
    const v = rec[field];
    if (typeof v === "string") rec[field] = subst(v, props);
  }
  if (rec.type === "stack") {
    rec.children = expandChildren(block.children, props, slots);
  }
  return rec;
}
function expandChildren(children, props, slots) {
  const out = [];
  for (const child of children ?? []) {
    if (child.type === "slot") {
      const filled = slots[child.name] ?? [];
      for (const b of filled) out.push(cloneBlock(b, props, slots));
    } else {
      out.push(cloneBlock(child, props, slots));
    }
  }
  return out;
}
var STRING_FIELDS = ["text", "label", "placeholder", "name", "href", "alt"];
function subst(value, props) {
  const m = /^@prop\.([A-Za-z0-9_-]+)$/.exec(value);
  if (m) {
    const key = m[1];
    const v = props[key];
    return v === void 0 ? value : String(v);
  }
  return value.replace(/@prop\.([A-Za-z0-9_-]+)/g, (whole, key) => {
    const v = props[key];
    return v === void 0 ? whole : String(v);
  });
}

// src/dsl/blocks.ts
function assetCacheKey(spec) {
  return `${spec.query}|${spec.kind ?? ""}|${spec.seed ?? ""}`;
}
function normalizeAssetSpec(src, defaults = {}) {
  if (typeof src === "string") return { query: src, ...defaults };
  return { ...defaults, ...src };
}
function lowerBlock(block, ctx, avail) {
  switch (block.type) {
    case "stack":
      return lowerStack(block, ctx, avail);
    case "text":
      return lowerText(block, ctx, avail);
    case "image":
      return lowerImage(block, ctx, avail);
    case "icon":
      return lowerIcon(block, ctx, avail);
    case "button":
      return lowerButton(block, ctx, avail);
    case "field":
      return lowerBlock(fieldToStack(block), ctx, avail);
    case "instance":
      return lowerInstance(block, ctx, avail);
    case "slot":
      return lowerLeaf(block, ctx, "frame", { w: 0, h: 0 });
    case "spacer": {
      const s = ctx.tokens.space(block.size, 16);
      return lowerLeaf(block, ctx, "rect", { w: s, h: s });
    }
    case "divider": {
      const w = resolveWidth(block.w, avail) ?? avail;
      const node = mkNode(block, ctx, "line");
      node.box = { w: round(w, 2), h: block.border?.width ?? 1 };
      node.stroke = ctx.tokens.colorRef(block.border?.color ?? "@border");
      node.strokeW = block.border?.width ?? 1;
      applyChildLayout(node, block);
      return { el: node.el, box: node.box };
    }
    default: {
      const _never = block;
      throw new Error(`lowerBlock: unhandled block ${JSON.stringify(_never)}`);
    }
  }
}
function mkNode(block, ctx, type) {
  const seed = block.id ?? block.name ?? type;
  const el = ctx.minter.mint(seed, type);
  const node = { id: `dsl:${el}`, el, type, box: { w: 0, h: 0 } };
  if (block.name) node.name = block.name;
  applyContainerStyle(node, block, ctx);
  ctx.nodes[el] = node;
  return node;
}
function applyContainerStyle(node, block, ctx) {
  if (block.bg !== void 0) {
    const plan = ctx.tokens.fillFor(block.bg);
    if (plan.fill) node.fill = plan.fill;
    if (plan.fills) node.fills = plan.fills;
    if (plan.assetId) node.assetId = plan.assetId;
  }
  if (block.border) {
    const c = ctx.tokens.resolveColor(block.border.color);
    node.stroke = ctx.tokens.colorRef(typeof c === "string" ? c : "@text");
    node.strokeW = block.border.width;
    if (block.border.align) node.strokeAlign = block.border.align;
    if (block.border.dash) node.strokeDash = block.border.dash;
  }
  const radius = ctx.tokens.radiusRef(block.radius);
  if (radius) node.radius = radius;
  if (block.blur !== void 0 && block.blur > 0) {
    const blurEffect = { type: "background-blur", radius: block.blur };
    const shadowEffects = ctx.tokens.shadowEffects(block.shadow);
    const stack = shadowEffects ? [...shadowEffects, blurEffect] : [blurEffect];
    node.effects = ctx.tokens.interner.internEffects(stack) ?? stack;
  } else {
    const shadow = ctx.tokens.shadowRef(block.shadow);
    if (shadow) node.shadow = shadow;
  }
  if (block.blend) node.blend = block.blend;
  if (block.opacity !== void 0 && block.opacity < 1) node.opacity = block.opacity;
  if (block.clip) node.clip = true;
}
function applyChildLayout(node, block) {
  if (block.grow) node.grow = block.grow;
  const self = mapSelf(block.self);
  if (self) node.selfAlign = self;
  const sizing = sizingFor(block.w, block.h);
  if (sizing) node.sizing = sizing;
  if (block.pos) node.pos = { x: round(block.pos.x, 2), y: round(block.pos.y, 2) };
  if (block.interactions?.length) {
    const motion = compileInteractions(block.interactions);
    if (motion.length) node.motion = motion;
  }
}
function lowerLeaf(block, ctx, type, box) {
  const node = mkNode(block, ctx, type);
  node.box = { w: round(box.w, 2), h: round(box.h, 2) };
  applyChildLayout(node, block);
  return { el: node.el, box: node.box };
}
function isFlexibleChild(c) {
  const w = c.w;
  const grow = c.grow;
  return w === "fill" || typeof grow === "number" && grow > 0;
}
function lowerRowChildren(children, ctx, avail, gap) {
  const n = children.length;
  const results = new Array(n).fill(void 0);
  const flexIdx = [];
  let fixedSum = 0;
  children.forEach((c, i) => {
    if (isFlexibleChild(c)) {
      flexIdx.push(i);
      return;
    }
    const r = lowerBlock(c, ctx, avail);
    results[i] = r;
    fixedSum += r.box.w;
  });
  if (flexIdx.length) {
    const totalGap = gap * Math.max(0, n - 1);
    const remainder = Math.max(0, avail - totalGap - fixedSum);
    const weights = flexIdx.map((i) => Math.max(1, children[i].grow ?? 1));
    const weightSum = weights.reduce((a, b) => a + b, 0) || 1;
    flexIdx.forEach((i, k) => {
      const share = remainder * (weights[k] / weightSum);
      results[i] = lowerBlock(children[i], ctx, Math.max(0, share));
    });
  }
  return results.map((r, i) => r ?? lowerBlock(children[i], ctx, avail));
}
function lowerStack(block, ctx, avail) {
  const node = mkNode(block, ctx, "frame");
  const flow = block.dir === "row" ? "row" : "col";
  const pad = ctx.tokens.pad(block.pad);
  const gap = ctx.tokens.space(block.gap);
  const gapCross = ctx.tokens.space(block.gapCross);
  const ownW = resolveWidth(block.w, avail);
  const inner = (ownW ?? avail) - pad[3] - pad[1];
  const childAvail = Math.max(inner, 0);
  const kids = flow === "row" && block.children.length > 1 ? lowerRowChildren(block.children, ctx, childAvail, gap) : block.children.map((c) => lowerBlock(c, ctx, childAvail));
  const layout = buildLayout(flow, {
    gap,
    gapCross,
    pad,
    justify: block.justify,
    align: block.align,
    wrap: block.wrap
  });
  const layoutRef = ctx.tokens.layoutRef(layout);
  if (layoutRef) node.layout = layoutRef;
  else node.layout = layout;
  const measured = measureContainer(
    flow,
    gap,
    pad,
    kids.map((k) => k.box)
  );
  const w = ownW ?? measured.w;
  const h = resolveHeight(block.h) ?? measured.h;
  node.box = { w: round(w, 2), h: round(h, 2) };
  node.children = kids.map((k) => k.el);
  applyChildLayout(node, block);
  return { el: node.el, box: node.box };
}
function lowerText(block, ctx, avail) {
  const node = mkNode(block, ctx, "text");
  const type = ctx.tokens.typeFor(block.style, "@text");
  if (type.ref) node.text = type.ref;
  const fixedW = resolveWidth(block.w, avail);
  const chars = flattenText(block.text);
  const wrapW = fixedW ?? avail;
  const box = measureText(chars, type.rts, wrapW, ctx.measurer);
  const fillPlan = block.color !== void 0 ? ctx.tokens.fillFor(block.color) : type.fillPlan;
  if (fillPlan?.fill) node.fill = fillPlan.fill;
  if (fillPlan?.fills) node.fills = fillPlan.fills;
  if (Array.isArray(block.text)) node.chars = buildRuns(block.text, ctx, type.ref);
  else node.chars = chars;
  if (type.decoration) node.textDecoration = type.decoration;
  if (block.textAlign) {
    node.textAlign = block.textAlign === "justify" ? "justified" : block.textAlign;
  }
  if (block.resize) node.textGrow = block.resize;
  else node.textGrow = fixedW === void 0 ? "wh" : "h";
  node.box = { w: round(fixedW ?? box.w, 2), h: round(box.h, 2) };
  applyChildLayout(node, block);
  return { el: node.el, box: node.box };
}
function flattenText(tv) {
  return typeof tv === "string" ? tv : tv.map((r) => r.t).join("");
}
function buildRuns(runs, ctx, dominant) {
  return runs.map((r) => {
    const out = { t: r.t };
    if (r.style) {
      const ref = ctx.tokens.typeFor(r.style).ref;
      if (ref && ref !== dominant) out.s = ref;
    }
    if (r.color !== void 0) out.c = ctx.tokens.colorRef(r.color);
    if (r.decoration) out.d = r.decoration;
    return out;
  });
}
function lowerImage(block, ctx, avail) {
  const spec = normalizeAssetSpec(block.src, { kind: "photo", role: "card-image" });
  const resolved = ctx.assets.get(assetCacheKey(spec));
  const node = mkNode(block, ctx, "rect");
  const w = resolveWidth(block.w, avail) ?? resolved?.w ?? spec.w ?? avail;
  let h = resolveHeight(block.h) ?? spec.h ?? resolved?.h;
  if (h === void 0) {
    const aspect = spec.aspect ?? (resolved?.w && resolved?.h ? resolved.w / resolved.h : 1.6);
    h = w / (aspect || 1.6);
  }
  if (resolved?.assetId) {
    const plan = ctx.tokens.imageFillFor(resolved.assetId, block.fit ?? resolved.scaleMode);
    node.fill = plan.fill;
    if (plan.fills) node.fills = plan.fills;
    node.assetId = plan.assetId;
  } else {
    node.fill = ctx.tokens.solidRef("#e5e7eb");
    ctx.warnings.push(`image "${spec.query}" unresolved \u2014 placeholder emitted`);
  }
  node.box = { w: round(w, 2), h: round(h, 2) };
  applyChildLayout(node, block);
  return { el: node.el, box: node.box };
}
function lowerIcon(block, ctx, _avail) {
  const spec = { query: block.name, kind: "icon", role: "icon" };
  const resolved = ctx.assets.get(assetCacheKey(spec));
  const size = block.size ?? 24;
  const colorTok = block.color ?? "@text";
  let node;
  if (resolved?.vectorPath) {
    node = mkNode(block, ctx, "vector");
    node.vectorPath = resolved.vectorPath;
    node.fill = ctx.tokens.colorRef(colorTok);
  } else if (resolved?.assetId) {
    node = mkNode(block, ctx, "rect");
    const plan = ctx.tokens.imageFillFor(resolved.assetId, "fit");
    node.fill = plan.fill;
    if (plan.fills) node.fills = plan.fills;
    node.assetId = plan.assetId;
  } else {
    node = mkNode(block, ctx, "vector");
    node.fill = ctx.tokens.colorRef(colorTok);
    ctx.warnings.push(`icon "${block.name}" unresolved \u2014 placeholder emitted`);
  }
  node.iconHint = block.name;
  node.box = { w: size, h: size };
  applyChildLayout(node, block);
  return { el: node.el, box: node.box };
}
var BTN_PAD = {
  sm: [8, 14],
  md: [10, 18],
  lg: [14, 24]
};
var BTN_ICON = { sm: 16, md: 18, lg: 20 };
function lowerButton(block, ctx, avail) {
  const stack = buttonToStack(block, ctx.brand);
  const lowered = lowerBlock(stack, ctx, avail);
  const node = ctx.nodes[lowered.el];
  if (node) node.pattern = "button";
  return lowered;
}
function buttonToStack(b, brand) {
  const variant = b.variant ?? "primary";
  const size = b.size ?? "md";
  const [pv, ph] = BTN_PAD[size];
  let bg;
  let border;
  let textColor = "@onPrimary";
  let decoration;
  switch (variant) {
    case "primary":
      bg = "@primary";
      textColor = "@onPrimary";
      break;
    case "secondary":
      bg = brand.colors.surface ? "@surface" : "@muted";
      textColor = "@text";
      break;
    case "outline":
      border = { color: "@primary", width: 1 };
      textColor = "@primary";
      break;
    case "ghost":
      textColor = "@primary";
      break;
    case "link":
      textColor = "@primary";
      decoration = "underline";
      break;
  }
  const children = [];
  const icon = b.icon ? { type: "icon", name: b.icon, size: BTN_ICON[size], color: textColor } : void 0;
  const labelSize = size === "lg" ? 17 : size === "sm" ? 14 : 15;
  const label = {
    type: "text",
    text: b.label,
    style: { size: labelSize, weight: 600, font: "body", ...decoration ? { decoration } : {} },
    color: textColor
  };
  if (icon && b.iconPos !== "right") children.push(icon);
  children.push(label);
  if (icon && b.iconPos === "right") children.push(icon);
  const link = variant === "link";
  const radius = b.radius ?? (brand.radius?.md !== void 0 ? "md" : 8);
  return {
    type: "stack",
    id: b.id,
    name: b.name ?? b.label,
    dir: "row",
    gap: 8,
    align: "center",
    justify: "center",
    pad: link ? 0 : [pv, ph],
    ...bg ? { bg } : {},
    ...border ? { border } : {},
    ...link ? {} : { radius },
    ...b.shadow ? { shadow: b.shadow } : {},
    ...b.w ? { w: b.w } : {},
    ...b.grow ? { grow: b.grow } : {},
    ...b.self ? { self: b.self } : {},
    ...b.pos ? { pos: b.pos } : {},
    ...b.interactions ? { interactions: b.interactions } : {},
    children
  };
}
function fieldToStack(f) {
  const kind = f.kind ?? "text";
  const inputChildren = [];
  if (f.placeholder) {
    inputChildren.push({
      type: "text",
      text: f.placeholder,
      style: { size: 15, weight: 400, font: "body" },
      color: "@muted"
    });
  }
  const input = {
    type: "stack",
    name: `${kind} input`,
    dir: "row",
    align: "center",
    pad: kind === "textarea" ? [12, 14, 40, 14] : [10, 14],
    w: "fill",
    bg: "@bg",
    border: { color: "@border", width: 1 },
    radius: 8,
    children: inputChildren
  };
  const children = [];
  if (f.label) {
    children.push({
      type: "text",
      text: f.label,
      style: { size: 14, weight: 500, font: "body" },
      color: "@text"
    });
  }
  children.push(input);
  return {
    type: "stack",
    id: f.id,
    name: f.name ?? f.label ?? "field",
    dir: "col",
    gap: 6,
    w: f.w ?? "fill",
    children
  };
}
function lowerInstance(block, ctx, avail) {
  const comp = ctx.components.get(block.component);
  if (!comp) {
    ctx.warnings.push(`instance of unknown component "${block.component}" \u2014 placeholder emitted`);
    return lowerLeaf(block, ctx, "frame", { w: avail, h: 0 });
  }
  const hasSlotContent = block.slots && Object.keys(block.slots).length > 0;
  if (hasSlotBlock(comp.body) || hasSlotContent) {
    return lowerFlattenedInstance(block, ctx, avail);
  }
  return lowerRealInstance(block, ctx, avail, comp);
}
function lowerFlattenedInstance(block, ctx, avail) {
  const expanded = ctx.expandInstance(block);
  if (!expanded) {
    ctx.warnings.push(`instance of unknown component "${block.component}" \u2014 placeholder emitted`);
    return lowerLeaf(block, ctx, "frame", { w: avail, h: 0 });
  }
  const lowered = lowerBlock(expanded.block, ctx, avail);
  const node = ctx.nodes[lowered.el];
  if (node) {
    node.component = { id: `dsl:${expanded.componentName}` };
    if (block.props) {
      const ref = ctx.tokens.interner.internProps(block.props);
      if (ref) node.props = ref;
    }
    ctx.onComponentInstance(expanded.componentName, lowered.el);
  }
  return lowered;
}
function lowerRealInstance(block, ctx, avail, comp) {
  let masterEl = ctx.componentMasters.get(comp.name);
  if (!masterEl) {
    const body = buildComponentMaster(comp);
    const lowered = lowerBlock(body, ctx, avail);
    const masterNode2 = ctx.nodes[lowered.el];
    if (masterNode2) masterNode2.type = "component";
    masterEl = lowered.el;
    ctx.componentMasters.set(comp.name, masterEl);
    ctx.componentLibraryEls.push(masterEl);
    ctx.onComponentInstance(comp.name, masterEl);
  }
  const masterNode = ctx.nodes[masterEl];
  const node = mkNode(block, ctx, "instance");
  node.component = { id: masterEl };
  if (block.props) {
    const ref = ctx.tokens.interner.internProps(block.props);
    if (ref) node.props = ref;
  }
  const w = resolveWidth(block.w, avail) ?? masterNode?.box.w ?? avail;
  const h = resolveHeight(block.h) ?? masterNode?.box.h ?? 0;
  node.box = { w: round(w, 2), h: round(h, 2) };
  applyChildLayout(node, block);
  return { el: node.el, box: node.box };
}

// src/dsl/sections.ts
var registry = /* @__PURE__ */ new Map();
function registerSection(role, lowerer) {
  registry.set(role, lowerer);
}
function lowerSection(section, ctx) {
  const lowerer = registry.get(section.role);
  if (!lowerer) throw new Error(`lowerSection: no lowerer registered for role "${section.role}"`);
  return lowerer(section, ctx);
}
function zeroHorizontal(pad) {
  if (!Array.isArray(pad)) return [pad, 0, pad, 0];
  if (pad.length === 2) return [pad[0], 0, pad[0], 0];
  return [pad[0], 0, pad[2], 0];
}
function band(section, content, contentDir = "col") {
  const bleed = !!section.bleed;
  const basePad = section.pad ?? [96, 24];
  const pad = bleed ? zeroHorizontal(basePad) : basePad;
  const inner = {
    type: "stack",
    name: "container",
    dir: contentDir,
    gap: section.gap ?? 24,
    align: section.align,
    justify: section.justify,
    w: bleed ? "fill" : section.maxWidth ? section.maxWidth : "fill",
    self: "center",
    children: content
  };
  return {
    type: "stack",
    id: section.id,
    name: section.role,
    dir: "col",
    align: "center",
    w: "fill",
    pad,
    ...section.bg !== void 0 ? { bg: section.bg } : {},
    children: [inner]
  };
}
function heading(text, style = "h2", textAlign) {
  return { type: "text", text, style, w: "fill", ...textAlign ? { textAlign } : {} };
}
function paragraph(text, color = "@muted", textAlign) {
  return { type: "text", text, style: "body", color, w: "fill", ...textAlign ? { textAlign } : {} };
}
function actionRow(actions) {
  if (!actions?.length) return void 0;
  return {
    type: "stack",
    name: "actions",
    dir: "row",
    gap: 12,
    align: "center",
    children: actions
  };
}
function cardBlock(card) {
  const children = [];
  if (card.image) {
    children.push({ type: "image", src: card.image, w: "fill", h: 180, radius: 8 });
  }
  if (card.icon) {
    children.push({
      type: "stack",
      name: "icon-chip",
      dir: "col",
      align: "center",
      justify: "center",
      w: 64,
      h: 64,
      bg: "@elevated",
      border: { color: "@border", width: 1 },
      radius: "full",
      children: [{ type: "icon", name: card.icon, size: 30, color: "@primary" }]
    });
  }
  children.push(heading(card.title, "h3"));
  if (card.body) children.push(paragraph(card.body));
  if (card.action) children.push(card.action);
  return {
    type: "stack",
    name: "card",
    dir: "col",
    gap: 12,
    pad: 24,
    w: "fill",
    bg: "@surface",
    // Hairline border + a soft ambient shadow give the card real elevation
    // (both drop silently if the brand lacks a `border` colour / `shadow` scale).
    border: { color: "@border", width: 1 },
    shadow: "lg",
    radius: 12,
    children
  };
}
function grid(cards, columns, columnRatios) {
  const ratioFor = (col) => {
    if (!columnRatios?.length) return 1;
    const r = columnRatios[col % columnRatios.length];
    return r && r > 0 ? r : 1;
  };
  const rows = [];
  for (let i = 0; i < cards.length; i += columns) {
    const rowCards = cards.slice(i, i + columns).map((c, col) => ({ ...cardBlock(c), grow: ratioFor(col) }));
    for (let pad = rowCards.length; pad < columns; pad += 1) {
      rowCards.push({ type: "stack", name: "col-spacer", dir: "col", grow: ratioFor(pad), children: [] });
    }
    rows.push({
      type: "stack",
      name: "row",
      dir: "row",
      gap: 24,
      w: "fill",
      children: rowCards
    });
  }
  return { type: "stack", name: "grid", dir: "col", gap: 24, w: "fill", children: rows };
}
registerSection("nav", (s) => {
  const brandChildren = [];
  if (s.brand?.logo) brandChildren.push({ type: "image", src: s.brand.logo, w: 120, h: 32 });
  if (s.brand?.text) brandChildren.push(heading(s.brand.text, "h3"));
  const brandStack = {
    type: "stack",
    name: "brand",
    dir: "row",
    gap: 8,
    align: "center",
    children: brandChildren
  };
  const links = {
    type: "stack",
    name: "links",
    dir: "row",
    gap: 24,
    align: "center",
    children: s.links.map((l) => ({
      type: "text",
      text: l.label,
      style: "body",
      ...l.href ? { interactions: [{ on: "click", url: l.href }] } : {}
    }))
  };
  const actions = actionRow(s.actions);
  const inner = {
    type: "stack",
    name: "container",
    dir: "row",
    justify: "between",
    align: "center",
    w: s.maxWidth ? s.maxWidth : "fill",
    self: "center",
    gap: 24,
    children: actions ? [brandStack, links, actions] : [brandStack, links]
  };
  return {
    type: "stack",
    id: s.id,
    name: "nav",
    dir: "col",
    align: "center",
    w: "fill",
    pad: s.pad ?? [16, 24],
    ...s.bg !== void 0 ? { bg: s.bg } : {},
    children: [inner]
  };
});
registerSection("hero", (s) => {
  const layout = s.layout ?? "center";
  const centered = layout === "center";
  const textAlign = centered ? "center" : void 0;
  const copy = [];
  if (s.eyebrow)
    copy.push({ type: "text", text: s.eyebrow, style: "label", color: "@primary", ...textAlign ? { textAlign } : {} });
  copy.push(heading(s.headline, "display", textAlign));
  if (s.sub) copy.push(paragraph(s.sub, "@muted", textAlign));
  const actions = actionRow(s.actions);
  if (actions) copy.push(actions);
  const copyStack = {
    type: "stack",
    name: "copy",
    dir: "col",
    gap: 20,
    grow: 1,
    align: centered ? "center" : "start",
    children: copy
  };
  if (centered) {
    return band(s, [copyStack]);
  }
  const media = s.media ?? { type: "image", src: { query: "hero", kind: "illustration" }, w: "fill", h: 400 };
  const mediaStack = { type: "stack", name: "media", dir: "col", grow: 1, w: "fill", children: [media] };
  const row = layout === "split-right" ? [copyStack, mediaStack] : [mediaStack, copyStack];
  return band(s, row, "row");
});
registerSection("card-grid", (s) => {
  const content = [];
  if (s.heading) content.push(heading(s.heading));
  content.push(grid(s.cards, s.columns ?? 3, s.columnRatios));
  return band(s, content);
});
registerSection("features", (s) => {
  const content = [];
  if (s.heading) content.push(heading(s.heading));
  if (s.layout === "alternating") {
    s.features.forEach((f, i) => {
      const text = {
        type: "stack",
        name: "feature-copy",
        dir: "col",
        gap: 12,
        grow: 1,
        children: [heading(f.title, "h3"), ...f.body ? [paragraph(f.body)] : []]
      };
      const media = f.image ? { type: "image", src: f.image, w: "fill", h: 280, radius: 12, grow: 1 } : { type: "icon", name: f.icon ?? "star", size: 64, color: "@primary" };
      const row = i % 2 === 0 ? [text, media] : [media, text];
      content.push({ type: "stack", name: "feature", dir: "row", gap: 48, align: "center", w: "fill", children: row });
    });
  } else {
    content.push(grid(s.features, Math.min(s.features.length, 3) || 1, s.columnRatios));
  }
  return band(s, content);
});
registerSection("form", (s) => {
  const content = [];
  if (s.heading) content.push(heading(s.heading));
  for (const f of s.fields) content.push({ ...f });
  content.push(s.submit);
  const form = band(s, content);
  const inner = form.children[0];
  inner.w = s.maxWidth ?? 480;
  return form;
});
registerSection("cta", (s) => {
  const subColor = s.bg !== void 0 ? "@onPrimary" : "@muted";
  const content = [heading(s.headline, "h1", "center")];
  if (s.sub) content.push(paragraph(s.sub, subColor, "center"));
  const actions = actionRow(s.actions);
  if (actions) content.push(actions);
  const cta = band(s, content);
  const inner = cta.children[0];
  inner.align = "center";
  return cta;
});
registerSection("footer", (s) => {
  const cols = (s.columns ?? []).map((col) => ({
    type: "stack",
    name: "footer-col",
    dir: "col",
    gap: 10,
    grow: 1,
    children: [
      { type: "text", text: col.title, style: "label", color: "@muted" },
      ...col.links.map((l) => ({
        type: "text",
        text: l.label,
        style: "body",
        ...l.href ? { interactions: [{ on: "click", url: l.href }] } : {}
      }))
    ]
  }));
  const brandChildren = [];
  if (s.brand?.logo) brandChildren.push({ type: "image", src: s.brand.logo, w: 120, h: 32 });
  if (s.brand?.text) brandChildren.push(heading(s.brand.text, "h3"));
  const topRow = {
    type: "stack",
    name: "footer-top",
    dir: "row",
    gap: 48,
    justify: "between",
    w: "fill",
    children: [
      // Bound the brand column so a long wordmark wraps within it instead of
      // running into the first link column.
      { type: "stack", name: "footer-brand", dir: "col", gap: 8, w: 260, children: brandChildren },
      ...cols
    ]
  };
  const content = [topRow];
  if (s.legal) content.push(paragraph(s.legal, "@muted"));
  return band(s, content);
});
registerSection("content", (s) => band(s, s.children));
registerSection("custom", (s) => {
  return band(s, [s.root]);
});

// src/dsl/tokens.ts
var DEFAULT_FONT = "Inter";
function autoTracking(size, transform) {
  if (transform === "upper") return round(size * 0.09, 2);
  if (size >= 44) return round(size * -0.03, 2);
  if (size >= 30) return round(size * -0.022, 2);
  if (size >= 22) return round(size * -0.014, 2);
  if (size >= 19) return round(size * -8e-3, 2);
  return 0;
}
var GRADIENT_KIND2 = {
  linear: "linear-gradient",
  radial: "radial-gradient",
  angular: "angular-gradient",
  diamond: "diamond-gradient"
};
function hexToRgba01(hex) {
  let h = hex.trim().replace(/^#/, "");
  if (h.length === 3 || h.length === 4) {
    h = h.split("").map((c) => c + c).join("");
  }
  const int = (a2, b) => {
    const v = parseInt(h.slice(a2, b), 16);
    return Number.isFinite(v) ? v / 255 : 0;
  };
  const a = h.length >= 8 ? int(6, 8) : 1;
  return { r: int(0, 2), g: int(2, 4), b: int(4, 6), a };
}
var TokenCtx = class {
  constructor(brand) {
    this.brand = brand;
  }
  brand;
  interner = new TokenInterner();
  /** Final token table — identical shape to `normalize()`'s output. */
  table() {
    return this.interner.table();
  }
  // -- Colour ---------------------------------------------------------------
  /** Resolve a `@role` ref / hex / Gradient down to a hex string or Gradient. */
  resolveColor(c) {
    if (typeof c !== "string") return c;
    if (c.startsWith("@")) {
      const role = c.slice(1);
      const v = this.brand.colors[role];
      if (v === void 0 || v === c) return "#000000";
      return this.resolveColor(v);
    }
    return c;
  }
  /** Intern a solid hex → `$cN`. */
  solidRef(hex) {
    return this.interner.internColor([{ type: "SOLID", color: hexToRgba01(hex) }]);
  }
  /** Plan the `fill` / `fills` fields for any DSL colour (solid or gradient). */
  fillFor(c) {
    const resolved = this.resolveColor(c);
    if (typeof resolved === "string") return { fill: this.solidRef(resolved) };
    const grad = this.gradientFill(resolved);
    return { fill: "gradient", fills: this.interner.internFills([grad]) };
  }
  /** Plan the `fill` / `fills` fields for an image paint. */
  imageFillFor(assetId, scaleMode) {
    const stack = [{ type: "image", assetId, scaleMode: scaleMode ?? "fill" }];
    return { fill: "image", fills: this.interner.internFills(stack), assetId };
  }
  gradientFill(g) {
    const out = {
      type: GRADIENT_KIND2[g.kind],
      stops: g.stops.map((s) => {
        const c = this.resolveColor(s.color);
        return { at: round(s.at, 4), color: typeof c === "string" ? c : "#000000" };
      })
    };
    if (g.kind === "linear" && g.angle !== void 0) out.angle = round(g.angle, 1);
    return out;
  }
  // -- Type -----------------------------------------------------------------
  /**
   * Resolve a type reference (a `Brand.type` name, an inline TypeStyle, or
   * nothing → body) into the `$tN` ref plus the derived colour ref, decoration,
   * and the concrete metrics a text measurer needs.
   */
  typeFor(style, fallbackColor) {
    const ts = this.resolveTypeStyle(style);
    const family = this.family(ts.font);
    const tracking = ts.tracking ?? autoTracking(ts.size, ts.transform);
    const fig = {
      fontFamily: family,
      fontWeight: ts.weight ?? 400,
      fontSize: ts.size,
      lineHeightPx: ts.line ? round(ts.size * ts.line, 2) : void 0,
      letterSpacing: tracking
    };
    const ref = this.interner.internText(fig);
    const colorTok = ts.color ?? fallbackColor;
    const color = colorTok !== void 0 ? this.colorRef(colorTok) : void 0;
    const fillPlan = colorTok !== void 0 ? this.fillFor(colorTok) : void 0;
    const rts = {
      size: ts.size,
      weight: ts.weight ?? 400,
      line: ts.line ?? 1.3,
      tracking,
      family
    };
    return {
      ref,
      color,
      fillPlan,
      decoration: ts.decoration,
      rts
    };
  }
  /** Resolve any colour to a solid `$cN` ref, flattening gradients to stop 0. */
  colorRef(c) {
    const resolved = this.resolveColor(c);
    if (typeof resolved === "string") return this.solidRef(resolved);
    const first = resolved.stops[0];
    const c0 = first ? this.resolveColor(first.color) : "#000000";
    return this.solidRef(typeof c0 === "string" ? c0 : "#000000");
  }
  resolveTypeStyle(style) {
    if (style && typeof style === "object") return style;
    if (typeof style === "string") {
      const named = this.brand.type[style];
      if (named) return named;
    }
    return this.brand.type.body ?? { size: 16, weight: 400, line: 1.5, font: "body" };
  }
  family(font) {
    const f = this.brand.fonts;
    if (font === "heading") return f?.heading ?? f?.body ?? DEFAULT_FONT;
    if (font === "mono") return f?.mono ?? "Menlo";
    if (font === "body" || font === void 0) return f?.body ?? DEFAULT_FONT;
    return font;
  }
  // -- Radius ---------------------------------------------------------------
  /** Resolve a radius token (px / "full" / scale key) → `$rN`. */
  radiusRef(tok) {
    if (tok === void 0) return void 0;
    if (tok === "full") return this.interner.internRadius("full");
    if (typeof tok === "number") return this.interner.internRadius(tok);
    const v = this.brand.radius?.[tok];
    if (v === void 0) return void 0;
    return this.interner.internRadius(v);
  }
  // -- Shadow ---------------------------------------------------------------
  /**
   * Resolve a shadow token. Scale keys intern to `$sN` (deduped); a raw CSS
   * box-shadow string is returned verbatim (valid for the compact `node.shadow`
   * field, just not deduped).
   */
  shadowRef(tok) {
    if (tok === void 0) return void 0;
    const spec = this.brand.shadow?.[tok] ?? DEFAULT_SHADOWS[tok];
    if (spec) return this.interner.internShadow([shadowToEffect(spec)]);
    return tok;
  }
  /**
   * Resolve a shadow scale-key token to a structured PDS `Effect[]` (rather
   * than the compact CSS-string `shadow` field) — needed when a `blur`
   * effect must ride in the same `node.effects` stack as the drop-shadow. A
   * raw CSS box-shadow string can't be losslessly re-parsed here, so it
   * returns `undefined` (caller drops the shadow in favor of the blur).
   */
  shadowEffects(tok) {
    if (tok === void 0) return void 0;
    const spec = this.brand.shadow?.[tok] ?? DEFAULT_SHADOWS[tok];
    if (!spec) return void 0;
    return [
      {
        type: spec.inset ? "inner-shadow" : "drop-shadow",
        x: spec.x,
        y: spec.y,
        blur: spec.blur,
        spread: spec.spread,
        color: spec.color
      }
    ];
  }
  // -- Layout ---------------------------------------------------------------
  /** Intern a fully-built PdsLayout → `$lN` (deduped across every container). */
  layoutRef(layout) {
    return this.interner.internLayout(layout);
  }
  // -- Spacing --------------------------------------------------------------
  /** Resolve a spacing token (px number or scale key) → px number. */
  space(s, dflt = 0) {
    if (s === void 0) return dflt;
    if (typeof s === "number") return s;
    const v = this.brand.spacing?.[s];
    return v ?? dflt;
  }
  /** Normalise a PadSpec into PDS `[top, right, bottom, left]`. */
  pad(p) {
    if (p === void 0) return [0, 0, 0, 0];
    if (!Array.isArray(p)) {
      const v = this.space(p);
      return [v, v, v, v];
    }
    if (p.length === 2) {
      const [v, h] = p;
      const vv = this.space(v);
      const hh = this.space(h);
      return [vv, hh, vv, hh];
    }
    const [t, r, b, l] = p;
    return [this.space(t), this.space(r), this.space(b), this.space(l)];
  }
};
var DEFAULT_SHADOWS = {
  sm: { x: 0, y: 1, blur: 2, spread: 0, color: "#00000026" },
  md: { x: 0, y: 4, blur: 12, spread: 0, color: "#00000033" },
  lg: { x: 0, y: 12, blur: 32, spread: -4, color: "#00000040" },
  xl: { x: 0, y: 24, blur: 56, spread: -8, color: "#0000004d" }
};
function shadowToEffect(s) {
  return {
    type: s.inset ? "INNER_SHADOW" : "DROP_SHADOW",
    color: hexToRgba01(s.color),
    offset: { x: s.x, y: s.y },
    radius: s.blur,
    spread: s.spread
  };
}

// src/dsl/compile.ts
var DEFAULT_PAGE_WIDTH = 1440;
async function compile(doc, ctx) {
  const warnings = [];
  const tokens = new TokenCtx(doc.brand);
  const componentIndex = indexComponents(doc.components);
  const pageWidth = ctx.page?.width ?? DEFAULT_PAGE_WIDTH;
  const pageBlocks = doc.pages.map((page) => ({
    page,
    block: pageToBlock(page, pageWidth, doc.brand)
  }));
  const specByKey = /* @__PURE__ */ new Map();
  for (const { block } of pageBlocks) collectAssets(block, componentIndex, specByKey);
  const resolved = await resolveAssets(specByKey, ctx.assets, warnings);
  const nodes = {};
  const minter = new HandleMinter();
  const sidecar = {};
  seedSidecar(doc.components, sidecar);
  const lowerCtx = {
    tokens,
    brand: doc.brand,
    minter,
    nodes,
    assets: resolved,
    measurer: ctx.measure,
    expandInstance: (inst) => expandInstance(inst, componentIndex),
    onComponentInstance: (name, el) => {
      const entry = sidecar[name];
      if (entry && !entry.el) entry.el = el;
    },
    components: componentIndex,
    componentMasters: /* @__PURE__ */ new Map(),
    componentLibraryEls: [],
    warnings
  };
  const roots = [];
  for (const { page, block } of pageBlocks) {
    const width = typeof page.width === "number" ? page.width : pageWidth;
    roots.push(lowerBlock(block, lowerCtx, width).el);
  }
  const allRoots = lowerCtx.componentLibraryEls.length ? [buildComponentLibraryFrame(lowerCtx.componentLibraryEls, lowerCtx), ...roots] : roots;
  const root = allRoots.length === 1 ? allRoots[0] : buildCanvas(allRoots, lowerCtx, pageWidth);
  const table = tokens.table();
  const estTokens = estimateTokens(JSON.stringify({ tokens: table, nodes }));
  const pdsDoc = {
    file: { name: doc.meta?.name ?? "Untitled", version: doc.version },
    root,
    tokens: table,
    nodes,
    meta: {
      nodeCount: Object.keys(nodes).length,
      estTokens,
      depthUsed: depthOf(root, nodes)
    },
    next: "Compiled from the Plumb Design DSL. Read `tokens` for the design system, then resolve `assetRequests`, lower to an EmitPlan, and apply-design. Re-serialize the built nodes and run plumb_verify to confirm the match."
  };
  return { doc: pdsDoc, components: sidecar, assetRequests: [...specByKey.values()], warnings };
}
function pageToBlock(page, defaultWidth, brand) {
  const children = page.sections.map((s) => lowerSection(s, { brand }));
  return {
    type: "stack",
    id: page.id ?? page.name,
    name: page.name,
    dir: "col",
    w: page.width ?? defaultWidth,
    ...typeof page.height === "number" ? { h: page.height } : {},
    ...page.bg !== void 0 ? { bg: page.bg } : {},
    children
  };
}
function buildComponentLibraryFrame(els, ctx) {
  const el = ctx.minter.mint("components", "frame");
  const kids = els.map((e) => ctx.nodes[e]).filter((n) => Boolean(n));
  const gap = 40;
  const pad = [40, 40, 40, 40];
  const innerW = kids.reduce((m, n) => Math.max(m, n.box.w), 0);
  const innerH = kids.reduce((s, n) => s + n.box.h, 0) + gap * Math.max(kids.length - 1, 0);
  ctx.nodes[el] = {
    id: `dsl:${el}`,
    el,
    type: "frame",
    name: "Components",
    box: { w: innerW + pad[1] + pad[3], h: innerH + pad[0] + pad[2] },
    layout: { flow: "col", pad, gap },
    children: els
  };
  return el;
}
function buildCanvas(pageEls, ctx, width) {
  const el = ctx.minter.mint("canvas", "frame");
  const kids = pageEls.map((p) => ctx.nodes[p]).filter((n) => Boolean(n));
  const totalH = kids.reduce((s, n) => s + n.box.h, 0) + 80 * Math.max(kids.length - 1, 0);
  ctx.nodes[el] = {
    id: `dsl:${el}`,
    el,
    type: "frame",
    box: { w: width, h: totalH },
    layout: { flow: "col", pad: [0, 0, 0, 0], gap: 80 },
    children: pageEls
  };
  return el;
}
function collectAssets(block, components, out) {
  switch (block.type) {
    case "image": {
      const spec = normalizeAssetSpec(block.src, { kind: "photo", role: "card-image" });
      out.set(assetCacheKey(spec), spec);
      break;
    }
    case "icon": {
      const spec = { query: block.name, kind: "icon", role: "icon" };
      out.set(assetCacheKey(spec), spec);
      break;
    }
    case "button": {
      if (block.icon) {
        const spec = { query: block.icon, kind: "icon", role: "icon" };
        out.set(assetCacheKey(spec), spec);
      }
      break;
    }
    case "stack": {
      for (const child of block.children) collectAssets(child, components, out);
      break;
    }
    case "instance": {
      const expanded = expandInstance(block, components);
      if (expanded) collectAssets(expanded.block, components, out);
      break;
    }
    default:
      break;
  }
}
async function resolveAssets(specByKey, resolver, warnings) {
  const out = /* @__PURE__ */ new Map();
  const entries = [...specByKey.entries()];
  const results = await Promise.all(
    entries.map(async ([key, spec]) => {
      try {
        return [key, await resolver.resolve(spec)];
      } catch (e) {
        warnings.push(`asset resolve failed for "${spec.query}": ${errMsg(e)}`);
        return [key, null];
      }
    })
  );
  for (const [key, res] of results) if (res) out.set(key, res);
  return out;
}
function seedSidecar(components, sidecar) {
  for (const c of components ?? []) {
    sidecar[c.name] = { id: `dsl:${c.name}`, el: "", props: c.props ?? [] };
  }
}
function depthOf(root, nodes) {
  const walk = (el, seen) => {
    if (seen.has(el)) return 0;
    seen.add(el);
    const node = nodes[el];
    if (!node?.children?.length) return 1;
    return 1 + Math.max(...node.children.map((c) => walk(c, seen)));
  };
  return walk(root, /* @__PURE__ */ new Set());
}
function errMsg(e) {
  return e instanceof Error ? e.message : String(e);
}

// src/emit/plan.ts
function lowerToEmitPlan(pds, opts) {
  const tokens = pds.tokens;
  const ops = [];
  const fonts = new FontSet();
  const assetRefs = /* @__PURE__ */ new Set();
  const visit = (el, parent, seen) => {
    if (seen.has(el)) return;
    seen.add(el);
    const node = pds.nodes[el];
    if (!node) return;
    const emit = lowerNode(node, tokens, fonts, assetRefs);
    ops.push({ key: el, parent, node: emit });
    for (const child of node.children ?? []) visit(child, el, seen);
  };
  visit(pds.root, null, /* @__PURE__ */ new Set());
  const assets = buildAssets(assetRefs, opts.assets);
  return {
    planId: opts.planId,
    target: opts.target,
    mode: opts.mode,
    prune: opts.prune,
    reveal: opts.reveal,
    fonts: fonts.list(),
    ...assets.length ? { assets } : {},
    ops
  };
}
var EMIT_TYPES = /* @__PURE__ */ new Set([
  "frame",
  "text",
  "rect",
  "ellipse",
  "line",
  "vector",
  "instance",
  "component",
  "group"
]);
function emitTypeOf(type) {
  return EMIT_TYPES.has(type) ? type : "frame";
}
function lowerNode(node, tokens, fonts, assetRefs) {
  const type = emitTypeOf(node.type);
  const en = {
    type,
    size: { w: node.box.w, h: node.box.h }
  };
  if (node.name) en.name = node.name;
  const layout = resolveLayout2(node.layout, tokens);
  if (layout) en.layout = toEmitLayout(layout);
  const child = toChildLayout(node);
  if (child) en.child = child;
  if (node.pos) en.pos = { x: node.pos.x, y: node.pos.y };
  const fills = resolveFills2(node, tokens, assetRefs);
  if (fills && fills.length) en.fills = fills;
  if (node.stroke) {
    const paint = solidPaint(resolveColor(node.stroke, tokens));
    if (paint) en.strokes = [paint];
  }
  if (node.strokeW !== void 0) en.strokeWeight = node.strokeW;
  if (node.strokeAlign) en.strokeAlign = node.strokeAlign.toUpperCase();
  if (node.strokeSides) en.strokeSides = node.strokeSides;
  if (node.strokeDash && node.strokeDash.length) en.dashPattern = node.strokeDash;
  const radius = resolveRadius(node.radius, tokens);
  if (radius !== void 0) en.cornerRadius = radius;
  const effects = resolveEffects2(node, tokens);
  if (effects && effects.length) en.effects = effects;
  if (node.opacity !== void 0) en.opacity = node.opacity;
  if (node.clip) en.clip = true;
  if (node.rotation !== void 0) en.rotation = node.rotation;
  if (node.blend) en.blendMode = node.blend.toUpperCase().replace(/-/g, "_");
  if (node.constraints) en.constraints = node.constraints;
  if (type === "vector" && node.vectorPath) {
    const d = resolveVector(node.vectorPath, tokens);
    if (d) en.vectorPaths = [{ data: d }];
  }
  if (type === "text") {
    const text = lowerText2(node, tokens, fonts);
    if (text) en.text = text;
    if (!en.fills && node.fill) {
      const paint = solidPaint(resolveColor(node.fill, tokens));
      if (paint) en.fills = [paint];
    }
  }
  if (type === "instance" && node.component) {
    en.instanceOf = typeof node.component === "string" ? node.component : node.component.id;
    const props = resolveProps(node.props, tokens);
    if (props) en.componentProps = props;
  }
  return en;
}
function resolveProps(props, tokens) {
  if (props === void 0) return void 0;
  if (typeof props === "string") return tokens.props?.[props];
  return props;
}
function resolveLayout2(layout, tokens) {
  if (layout === void 0) return void 0;
  if (typeof layout === "string") return tokens.layout?.[layout];
  return layout;
}
var JUSTIFY_TO_PRIMARY = {
  "flex-start": "MIN",
  center: "CENTER",
  "flex-end": "MAX",
  "space-between": "SPACE_BETWEEN"
};
var ALIGN_TO_COUNTER = {
  "flex-start": "MIN",
  center: "CENTER",
  "flex-end": "MAX",
  baseline: "BASELINE"
};
function toEmitLayout(l) {
  const [t, r, b, left] = l.pad;
  const out = {
    mode: l.flow === "row" ? "HORIZONTAL" : "VERTICAL",
    pad: { t, r, b, l: left }
  };
  if (l.gap !== void 0) out.gap = l.gap;
  if (l.gapCross !== void 0) out.gapCross = l.gapCross;
  if (l.justify && JUSTIFY_TO_PRIMARY[l.justify]) out.primary = JUSTIFY_TO_PRIMARY[l.justify];
  if (l.align && ALIGN_TO_COUNTER[l.align]) out.counter = ALIGN_TO_COUNTER[l.align];
  if (l.wrap) out.wrap = true;
  return out;
}
var SELF_TO_ALIGN = {
  stretch: "STRETCH",
  min: "MIN",
  center: "CENTER",
  max: "MAX"
};
function toChildLayout(node) {
  const out = {};
  let set = false;
  if (node.grow) {
    out.grow = node.grow;
    set = true;
  }
  if (node.selfAlign && SELF_TO_ALIGN[node.selfAlign]) {
    out.align = SELF_TO_ALIGN[node.selfAlign];
    set = true;
  }
  if (node.sizing?.w) {
    out.sizingH = node.sizing.w === "fill" ? "FILL" : node.sizing.w === "fixed" ? "FIXED" : "HUG";
    set = true;
  }
  if (node.sizing?.h) {
    out.sizingV = node.sizing.h === "fill" ? "FILL" : node.sizing.h === "fixed" ? "FIXED" : "HUG";
    set = true;
  }
  return set ? out : void 0;
}
function resolveColor(ref, tokens) {
  if (ref.startsWith("$")) return tokens.color[ref];
  if (ref.startsWith("#")) return ref;
  return void 0;
}
function solidPaint(hex) {
  if (!hex) return void 0;
  const c = hexToRgba01(hex);
  const paint = { type: "SOLID", color: { r: c.r, g: c.g, b: c.b } };
  const a = c.a ?? 1;
  if (a < 1) paint.opacity = a;
  return paint;
}
var SCALE_MODE2 = {
  fill: "FILL",
  fit: "FIT",
  crop: "CROP",
  tile: "TILE",
  stretch: "FILL"
};
function resolveFills2(node, tokens, assetRefs) {
  const stack = resolveFillStack(node.fills, tokens);
  if (stack) {
    const out = [];
    for (const f of stack) {
      const p = fillToPaint(f, assetRefs);
      if (p) out.push(p);
    }
    return out.length ? out : void 0;
  }
  if (node.fill && node.fill.startsWith("$c")) {
    const p = solidPaint(resolveColor(node.fill, tokens));
    return p ? [p] : void 0;
  }
  return void 0;
}
function resolveFillStack(fills, tokens) {
  if (fills === void 0) return void 0;
  if (typeof fills === "string") return tokens.fills?.[fills];
  return fills;
}
function fillToPaint(f, assetRefs) {
  if (f.type === "color") return solidFillToPaint(f);
  if (f.type === "image") return imageFillToPaint(f, assetRefs);
  return gradientFillToPaint(f);
}
function solidFillToPaint(f) {
  const c = hexToRgba01(f.color);
  const paint = { type: "SOLID", color: { r: c.r, g: c.g, b: c.b } };
  const alpha = (c.a ?? 1) * (f.opacity ?? 1);
  if (alpha < 1) paint.opacity = alpha;
  if (f.var) paint.boundVar = f.var;
  return paint;
}
function imageFillToPaint(f, assetRefs) {
  if (!f.assetId) return void 0;
  assetRefs.add(f.assetId);
  const paint = { type: "IMAGE", assetRef: f.assetId };
  if (f.scaleMode && SCALE_MODE2[f.scaleMode]) paint.scaleMode = SCALE_MODE2[f.scaleMode];
  if (f.opacity !== void 0 && f.opacity < 1) paint.opacity = f.opacity;
  return paint;
}
var GRADIENT_TYPE = {
  "linear-gradient": "GRADIENT_LINEAR",
  "radial-gradient": "GRADIENT_RADIAL",
  "angular-gradient": "GRADIENT_ANGULAR",
  "diamond-gradient": "GRADIENT_DIAMOND"
};
function gradientFillToPaint(f) {
  const type = GRADIENT_TYPE[f.type];
  const stops = f.stops.map((s) => {
    const c = hexToRgba01(s.color);
    return { position: s.at, color: { r: c.r, g: c.g, b: c.b, a: c.a ?? 1 } };
  });
  const paint = { type, stops };
  if (f.type === "linear-gradient" && f.angle !== void 0) {
    paint.transform = linearGradientTransform(f.angle);
  }
  if (f.opacity !== void 0 && f.opacity < 1) paint.opacity = f.opacity;
  return paint;
}
function linearGradientTransform(angleDeg) {
  const phi = (angleDeg - 90) * Math.PI / 180;
  const dx = Math.cos(phi);
  const dy = Math.sin(phi);
  const h0x = 0.5 - dx * 0.5;
  const h0y = 0.5 - dy * 0.5;
  return [
    [dx, dy, -(dx * h0x + dy * h0y)],
    [-dy, dx, dy * h0x - dx * h0y]
  ];
}
function resolveRadius(radius, tokens) {
  if (radius === void 0) return void 0;
  if (Array.isArray(radius)) return radius;
  if (radius.startsWith("$")) {
    const v = tokens.radius[radius];
    if (v === void 0) return void 0;
    return v === "full" ? 9999 : v;
  }
  const n = Number(radius);
  return Number.isFinite(n) ? n : void 0;
}
function resolveEffects2(node, tokens) {
  let stack;
  if (Array.isArray(node.effects)) stack = node.effects;
  else if (typeof node.effects === "string") stack = tokens.effects?.[node.effects];
  if (stack && stack.length) {
    const out = [];
    for (const e of stack) {
      const em = effectToEmit(e);
      if (em) out.push(em);
    }
    return out.length ? out : void 0;
  }
  if (node.shadow) {
    const css = node.shadow.startsWith("$") ? tokens.shadow[node.shadow] : node.shadow;
    if (css) return parseBoxShadow(css);
  }
  return void 0;
}
function effectToEmit(e) {
  if (e.type === "drop-shadow" || e.type === "inner-shadow") {
    const c = hexToRgba01(e.color);
    return {
      type: e.type === "drop-shadow" ? "DROP_SHADOW" : "INNER_SHADOW",
      color: { r: c.r, g: c.g, b: c.b, a: c.a ?? 1 },
      offset: { x: e.x, y: e.y },
      radius: e.blur,
      spread: e.spread
    };
  }
  if (e.type === "layer-blur" || e.type === "background-blur") {
    return {
      type: e.type === "layer-blur" ? "LAYER_BLUR" : "BACKGROUND_BLUR",
      radius: e.radius
    };
  }
  return void 0;
}
function parseBoxShadow(css) {
  const out = [];
  for (const raw of splitShadows(css)) {
    const part = raw.trim();
    if (!part) continue;
    const inset = part.startsWith("inset");
    const body = inset ? part.slice(5).trim() : part;
    const m = /^(-?[\d.]+)px\s+(-?[\d.]+)px\s+(-?[\d.]+)px\s+(-?[\d.]+)px\s+(#[0-9a-fA-F]+)$/.exec(body);
    if (!m) continue;
    const c = hexToRgba01(m[5]);
    out.push({
      type: inset ? "INNER_SHADOW" : "DROP_SHADOW",
      color: { r: c.r, g: c.g, b: c.b, a: c.a ?? 1 },
      offset: { x: Number(m[1]), y: Number(m[2]) },
      radius: Number(m[3]),
      spread: Number(m[4])
    });
  }
  return out;
}
function splitShadows(css) {
  return css.split(/,(?![^(]*\))/);
}
function resolveVector(vector, tokens) {
  if (vector.startsWith("$")) return tokens.vector?.[vector];
  return vector;
}
function resolveType(ref, tokens) {
  if (!ref) return void 0;
  const raw = ref.startsWith("$") ? tokens.text[ref] : ref;
  if (!raw) return void 0;
  const m = /^(\d+)\s+([\d.]+)px(?:\/([\d.]+))?(.*)$/.exec(raw.trim());
  if (!m) return void 0;
  const weight = Number(m[1]);
  const size = Number(m[2]);
  const lhRatio = m[3] !== void 0 ? Number(m[3]) : void 0;
  let rest = (m[4] ?? "").trim();
  let tracking;
  const tm = /\s?(-?[\d.]+)px$/.exec(rest);
  if (tm) {
    tracking = Number(tm[1]);
    rest = rest.slice(0, tm.index).trim();
  }
  const family = rest || "Inter";
  const out = { family, weight, size };
  if (lhRatio !== void 0) out.lineHeightPx = round22(lhRatio * size);
  if (tracking !== void 0) out.tracking = tracking;
  return out;
}
function lowerText2(node, tokens, fonts) {
  const metrics = resolveType(node.text, tokens) ?? {
    family: "Inter",
    weight: 400,
    size: 16
  };
  const font = faceFor(metrics.family, metrics.weight);
  fonts.add(font);
  const characters = charsToString(node.chars);
  const out = {
    characters,
    font,
    fontSize: metrics.size
  };
  if (metrics.lineHeightPx !== void 0) out.lineHeightPx = metrics.lineHeightPx;
  if (metrics.tracking !== void 0) out.letterSpacing = metrics.tracking;
  if (node.textAlign) out.align = node.textAlign.toUpperCase();
  if (node.textDecoration) {
    out.decoration = node.textDecoration === "underline" ? "UNDERLINE" : "STRIKETHROUGH";
  }
  out.autoResize = textAutoResize(node.textGrow);
  const runs = buildRuns2(node.chars, tokens, fonts, metrics);
  if (runs.length) out.runs = runs;
  return out;
}
function charsToString(chars) {
  if (chars === void 0) return "";
  if (typeof chars === "string") return chars;
  return chars.map((r) => r.t).join("");
}
function textAutoResize(grow) {
  switch (grow) {
    case "h":
      return "HEIGHT";
    case "wh":
      return "WIDTH_AND_HEIGHT";
    case "trunc":
      return "TRUNCATE";
    default:
      return "NONE";
  }
}
function buildRuns2(chars, tokens, fonts, base) {
  if (!Array.isArray(chars)) return [];
  const runs = [];
  let cursor = 0;
  for (const r of chars) {
    const start = cursor;
    const end = cursor + r.t.length;
    cursor = end;
    if (end <= start) continue;
    const run = runOverrides(r, start, end, tokens, fonts, base);
    if (run) runs.push(run);
  }
  return runs;
}
function runOverrides(r, start, end, tokens, fonts, base) {
  const run = { start, end };
  let set = false;
  if (r.s) {
    const m = resolveType(r.s, tokens);
    if (m) {
      const face = faceFor(m.family, m.weight);
      fonts.add(face);
      run.font = face;
      run.fontSize = m.size;
      if (m.lineHeightPx !== void 0) run.lineHeightPx = m.lineHeightPx;
      if (m.tracking !== void 0) run.letterSpacing = m.tracking;
      set = true;
    }
  }
  if (r.c) {
    const paint = solidPaint(resolveColor(r.c, tokens));
    if (paint) {
      run.fills = [paint];
      set = true;
    }
  }
  if (r.d) {
    run.decoration = r.d === "underline" ? "UNDERLINE" : "STRIKETHROUGH";
    set = true;
  }
  void base;
  return set ? run : void 0;
}
var WEIGHT_STYLE = {
  100: "Thin",
  200: "Extra Light",
  300: "Light",
  400: "Regular",
  500: "Medium",
  600: "Semi Bold",
  700: "Bold",
  800: "Extra Bold",
  900: "Black"
};
function faceFor(family, weight) {
  const bucket = Math.max(100, Math.min(900, Math.round(weight / 100) * 100));
  return { family, style: WEIGHT_STYLE[bucket] ?? "Regular" };
}
var FontSet = class {
  seen = /* @__PURE__ */ new Set();
  faces = [];
  add(f) {
    const k = `${f.family}\0${f.style}`;
    if (this.seen.has(k)) return;
    this.seen.add(k);
    this.faces.push(f);
  }
  list() {
    return this.faces.slice();
  }
};
function buildAssets(refs, info) {
  if (!info) return [];
  const out = [];
  for (const ref of refs) {
    const a = info[ref];
    if (!a) continue;
    const asset = { ref, ext: a.ext, kind: a.kind };
    if (a.svgInline) asset.svgInline = a.svgInline;
    if (a.w !== void 0) asset.w = a.w;
    if (a.h !== void 0) asset.h = a.h;
    out.push(asset);
  }
  return out;
}
function round22(n) {
  return Math.round(n * 100) / 100;
}

// src/brand/references.ts
var CATALOGUE = [
  // ── Dev tools / SaaS / dashboards ──────────────────────────────────────
  { url: "https://linear.app", name: "Linear", why: "The reference for dark, keyboard-first product UI \u2014 LCH color system, razor type, restrained motion.", tags: ["saas", "dashboard", "dev-tool", "productivity", "dark", "minimal", "premium", "b2b"] },
  { url: "https://vercel.com", name: "Vercel", why: "Geist system: near-black ground, a single blue accent, a strict neutral ramp, sharp corners.", tags: ["saas", "dev-tool", "platform", "dark", "minimal", "premium", "infrastructure"] },
  { url: "https://stripe.com", name: "Stripe", why: "The gold standard for fintech marketing \u2014 the animated mesh gradient, S\xF6hne type, layered soft shadows.", tags: ["fintech", "finance", "payments", "saas", "b2b", "gradient", "premium", "light"] },
  { url: "https://www.mercury.com", name: "Mercury", why: "Premium fintech with a calm, editorial palette and generous whitespace \u2014 banking that feels designed.", tags: ["fintech", "finance", "banking", "b2b", "premium", "light", "editorial"] },
  { url: "https://ramp.com", name: "Ramp", why: "Confident finance product marketing \u2014 bold type scale, tight grid, restrained accent.", tags: ["fintech", "finance", "saas", "b2b", "premium", "spend", "corporate"] },
  { url: "https://posthog.com", name: "PostHog", why: "Analytics/dashboard product with dense data UI treated with real craft.", tags: ["analytics", "dashboard", "dev-tool", "saas", "data", "b2b"] },
  { url: "https://resend.com", name: "Resend", why: "Minimal developer product \u2014 beautiful restraint, mono accents, perfect spacing rhythm.", tags: ["dev-tool", "saas", "email", "minimal", "dark", "premium", "infrastructure"] },
  // ── AI products ────────────────────────────────────────────────────────
  { url: "https://openai.com", name: "OpenAI", why: "Restrained, near-monochrome AI brand \u2014 enormous whitespace, quiet confidence.", tags: ["ai", "product", "minimal", "light", "premium", "research"] },
  { url: "https://www.anthropic.com", name: "Anthropic", why: "Warm off-white editorial system with a serif/grotesque pairing \u2014 human, considered AI.", tags: ["ai", "product", "editorial", "warm", "light", "premium", "research"] },
  // ── Luxury / brand / automotive ────────────────────────────────────────
  { url: "https://www.apple.com", name: "Apple", why: "The single-hero-product discipline \u2014 full-bleed cinematic imagery, SF Pro, scroll storytelling.", tags: ["luxury", "consumer", "product", "hardware", "premium", "cinematic", "automotive", "minimal"] },
  { url: "https://www.porsche.com", name: "Porsche", why: "Automotive luxury \u2014 dark cinematic photography, tight type, one metallic accent.", tags: ["luxury", "automotive", "car", "premium", "dark", "cinematic", "brand"] },
  { url: "https://www.teenage.engineering", name: "Teenage Engineering", why: "Constraint as identity \u2014 monospace, a tiny palette, playful industrial rigor.", tags: ["brand", "hardware", "product", "playful", "editorial", "minimal", "constrained"] },
  // ── Agency / portfolio / awwwards-caliber ──────────────────────────────
  { url: "https://lusion.co", name: "Lusion", why: "Most-decorated WebGL studio \u2014 the ceiling for signature interactive moments.", tags: ["agency", "portfolio", "creative", "awwwards", "webgl", "3d", "experimental", "dark"] },
  { url: "https://www.active-theory.com", name: "Active Theory", why: "Immersive, cinematic interactive work \u2014 reference for a signature hero moment.", tags: ["agency", "portfolio", "creative", "awwwards", "webgl", "3d", "experimental", "immersive"] },
  // ── Commerce / consumer ────────────────────────────────────────────────
  { url: "https://www.gymshark.com", name: "Gymshark", why: "Bold consumer commerce \u2014 strong imagery, high-contrast type, confident CTAs.", tags: ["ecommerce", "retail", "consumer", "brand", "bold", "commerce"] },
  { url: "https://www.allbirds.com", name: "Allbirds", why: "Clean, natural commerce \u2014 soft neutral palette, generous product photography.", tags: ["ecommerce", "retail", "consumer", "natural", "light", "commerce", "sustainable"] }
];
var STOP = /* @__PURE__ */ new Set(["a", "an", "the", "for", "with", "and", "of", "to", "app", "site", "website", "page", "landing", "build", "make", "create", "design"]);
function briefTokens(brief) {
  const bag = [brief.description ?? "", brief.industry ?? "", ...brief.keywords ?? [], ...brief.tone ?? []].join(" ").toLowerCase();
  const words = bag.split(/[^a-z0-9+]+/).filter((w) => w.length > 1 && !STOP.has(w));
  return words;
}
function userRef(url) {
  let host = url;
  try {
    host = new URL(url.startsWith("http") ? url : `https://${url}`).hostname.replace(/^www\./, "");
  } catch {
  }
  return {
    url: url.startsWith("http") ? url : `https://${url}`,
    name: host,
    why: "Reference supplied in the brief.",
    tags: []
  };
}
var SYNONYMS = {
  bank: ["fintech", "finance"],
  banking: ["fintech", "finance"],
  finance: ["fintech"],
  financial: ["fintech", "finance"],
  payment: ["fintech", "payments"],
  payments: ["fintech"],
  admin: ["dashboard", "saas"],
  analytics: ["dashboard", "data"],
  developer: ["dev-tool"],
  devtool: ["dev-tool"],
  infra: ["infrastructure", "platform"],
  car: ["automotive"],
  vehicle: ["automotive"],
  auto: ["automotive"],
  shop: ["ecommerce", "commerce"],
  store: ["ecommerce", "commerce"],
  retail: ["ecommerce"],
  agency: ["agency", "portfolio"],
  studio: ["agency", "portfolio", "creative"],
  portfolio: ["portfolio", "creative"],
  award: ["awwwards", "creative"],
  awwwards: ["awwwards", "creative", "experimental"],
  llm: ["ai"],
  premium: ["premium", "luxury"],
  luxury: ["luxury", "premium"]
};
function discoverReferences(brief, count = 4) {
  const picked = [];
  const seen = /* @__PURE__ */ new Set();
  const take = (r) => {
    if (seen.has(r.name)) return;
    seen.add(r.name);
    picked.push(r);
  };
  for (const u of brief.references ?? []) take(userRef(u));
  const tokens = briefTokens(brief);
  const wants = /* @__PURE__ */ new Map();
  for (const t of tokens) {
    wants.set(t, (wants.get(t) ?? 0) + 1);
    for (const syn of SYNONYMS[t] ?? []) wants.set(syn, (wants.get(syn) ?? 0) + 1);
  }
  const scored = CATALOGUE.map((r) => {
    let score = 0;
    for (const tag of r.tags) if (wants.has(tag)) score += wants.get(tag);
    if (r.tags.includes("premium")) score += 0.1;
    return { r, score };
  }).sort((a, b) => b.score - a.score);
  for (const { r, score } of scored) {
    if (picked.length >= count) break;
    if (score <= 0) continue;
    take(r);
  }
  if (picked.length < count) {
    for (const name of ["Linear", "Stripe", "Apple", "Vercel"]) {
      if (picked.length >= count) break;
      const r = CATALOGUE.find((c) => c.name === name);
      if (r) take(r);
    }
  }
  return picked.slice(0, count);
}

// src/brand/palette.ts
var SAMPLE_SCRIPT = `(() => {
  const hex = (r,g,b) => '#' + [r,g,b].map(x => Math.max(0,Math.min(255,x)).toString(16).padStart(2,'0')).join('');
  const parse = (s) => {
    const m = /rgba?\\(([^)]+)\\)/.exec(s || ''); if (!m) return null;
    const p = m[1].split(',').map(x => parseFloat(x));
    if (p.length >= 4 && p[3] === 0) return null;      // fully transparent
    if (p.some(n => Number.isNaN(n))) return null;
    return hex(Math.round(p[0]), Math.round(p[1]), Math.round(p[2]));
  };
  const bg = new Map(), tx = new Map(), bd = new Map();
  const add = (map, h, w) => { if (h) map.set(h, (map.get(h) || 0) + w); };
  const els = document.querySelectorAll('body *');
  let n = 0;
  for (const el of els) {
    if (n++ > 5000) break;
    const r = el.getBoundingClientRect();
    if (r.width < 4 || r.height < 4 || r.bottom < 0 || r.top > 6000) continue;
    const cs = getComputedStyle(el);
    add(bg, parse(cs.backgroundColor), Math.min(r.width * r.height, 2500000));
    const t = (el.textContent || '').trim();
    if (t.length > 0 && t.length < 400) add(tx, parse(cs.color), r.width * 24);
    if (parseFloat(cs.borderTopWidth) > 0) add(bd, parse(cs.borderTopColor), r.width);
  }
  const top = (map, k) => [...map.entries()].sort((a,b) => b[1]-a[1]).slice(0,k).map(([hex,w]) => ({hex,w}));
  return { bg: top(bg,10), text: top(tx,10), border: top(bd,8) };
})()`;
async function extractRawPalette(b) {
  const raw = await evaluate(b, SAMPLE_SCRIPT);
  return { bg: raw?.bg ?? [], text: raw?.text ?? [], border: raw?.border ?? [] };
}
function rgb(hex) {
  const h = hex.replace("#", "");
  return [parseInt(h.slice(0, 2), 16), parseInt(h.slice(2, 4), 16), parseInt(h.slice(4, 6), 16)];
}
function lum([r, g, b]) {
  const f = (c) => {
    const s = c / 255;
    return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
  };
  return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b);
}
function chroma([r, g, b]) {
  const mx = Math.max(r, g, b), mn = Math.min(r, g, b);
  return mx === 0 ? 0 : (mx - mn) / mx;
}
function mix(a, b, t) {
  const [r1, g1, b1] = rgb(a), [r2, g2, b2] = rgb(b);
  const h = (x) => Math.round(x).toString(16).padStart(2, "0");
  return `#${h(r1 + (r2 - r1) * t)}${h(g1 + (g2 - g1) * t)}${h(b1 + (b2 - b1) * t)}`;
}
function contrast(a, b) {
  const la = lum(rgb(a)) + 0.05, lb = lum(rgb(b)) + 0.05;
  return la > lb ? la / lb : lb / la;
}
function ensureContrast(fg, bg, target, toward) {
  let out = fg;
  for (let i = 0; i < 16 && contrast(out, bg) < target; i += 1) {
    out = mix(out, toward, 0.1);
  }
  return out;
}
function hue([r, g, b]) {
  const mx = Math.max(r, g, b), mn = Math.min(r, g, b), d = mx - mn;
  if (d === 0) return 0;
  let h;
  if (mx === r) h = (g - b) / d % 6;
  else if (mx === g) h = (b - r) / d + 2;
  else h = (r - g) / d + 4;
  h *= 60;
  return h < 0 ? h + 360 : h;
}
function synthesizeBrand(raw) {
  const bgHex = raw.bg[0]?.hex ?? "#0A0A0B";
  const dark = lum(rgb(bgHex)) < 0.35;
  const white = "#FFFFFF", black = "#000000";
  const surface = mix(bgHex, dark ? white : black, dark ? 0.05 : 0.03);
  const elevated = mix(bgHex, dark ? white : black, dark ? 0.1 : 0.06);
  const border = raw.border.map((c) => c.hex).find((h) => {
    const cr = contrast(h, bgHex);
    return cr >= 1.08 && cr <= 3 && chroma(rgb(h)) < 0.4;
  }) ?? mix(bgHex, dark ? white : black, dark ? 0.16 : 0.12);
  const textCand = raw.text.find((c) => contrast(c.hex, bgHex) >= 4.5)?.hex;
  const text = textCand ?? (dark ? "#F4F4F5" : "#0A0A0B");
  const mutedCand = raw.text.map((c) => c.hex).find((h) => {
    const cr = contrast(h, bgHex);
    return cr >= 2 && cr < 4.5 && chroma(rgb(h)) < 0.22;
  }) ?? mix(text, bgHex, 0.45);
  const muted = ensureContrast(mutedCand, elevated, 4.5, text);
  const all = [...raw.bg, ...raw.text, ...raw.border];
  let accent = "";
  let bestScore = 0;
  for (const c of all) {
    const px = rgb(c.hex);
    const ch = chroma(px);
    const l = lum(px);
    if (ch < 0.3 || l < 0.03 || l > 0.95) continue;
    const score = ch * Math.sqrt(Math.max(1, c.w));
    if (score > bestScore) {
      bestScore = score;
      accent = c.hex;
    }
  }
  if (!accent) accent = dark ? "#6E7BFF" : "#3B5BFF";
  const onPrimary = contrast(white, accent) >= contrast(black, accent) ? "#FFFFFF" : "#0A0A0B";
  return { bg: bgHex, surface, elevated, text, muted, primary: accent, onPrimary, border, accent };
}
function synthesizeBrandFromMany(raws) {
  const valid = raws.filter((r) => r.bg.length || r.text.length || r.border.length);
  if (valid.length <= 1) return synthesizeBrand(valid[0] ?? { bg: [], text: [], border: [] });
  const white = "#FFFFFF", black = "#000000";
  const bgCandidates = valid.map((r) => r.bg[0]).filter((c) => !!c);
  if (bgCandidates.length === 0) return synthesizeBrand(valid[0]);
  const darkVotes = bgCandidates.filter((c) => lum(rgb(c.hex)) < 0.35).length;
  const wantDark = darkVotes * 2 >= bgCandidates.length;
  const modeMatched = bgCandidates.filter((c) => lum(rgb(c.hex)) < 0.35 === wantDark);
  const bgPool = modeMatched.length ? modeMatched : bgCandidates;
  const bgByLum = [...bgPool].sort((a, b) => lum(rgb(a.hex)) - lum(rgb(b.hex)));
  const bgHex = bgByLum[Math.floor(bgByLum.length / 2)].hex;
  const dark = lum(rgb(bgHex)) < 0.35;
  const surface = mix(bgHex, dark ? white : black, dark ? 0.05 : 0.03);
  const elevated = mix(bgHex, dark ? white : black, dark ? 0.1 : 0.06);
  const border = valid.flatMap((r) => r.border).map((c) => c.hex).find((h) => {
    const cr = contrast(h, bgHex);
    return cr >= 1.08 && cr <= 3 && chroma(rgb(h)) < 0.4;
  }) ?? mix(bgHex, dark ? white : black, dark ? 0.16 : 0.12);
  const textCandidates = valid.flatMap((r) => r.text).filter((c) => contrast(c.hex, bgHex) >= 4.5);
  let text;
  if (textCandidates.length) {
    const byLum = [...textCandidates].sort((a, b) => lum(rgb(a.hex)) - lum(rgb(b.hex)));
    text = byLum[Math.floor(byLum.length / 2)].hex;
  } else {
    text = dark ? "#F4F4F5" : "#0A0A0B";
  }
  const mutedCand = valid.flatMap((r) => r.text).map((c) => c.hex).find((h) => {
    const cr = contrast(h, bgHex);
    return cr >= 2 && cr < 4.5 && chroma(rgb(h)) < 0.22;
  }) ?? mix(text, bgHex, 0.45);
  const muted = ensureContrast(mutedCand, elevated, 4.5, text);
  const allColors = valid.flatMap((r) => [...r.bg, ...r.text, ...r.border]);
  const buckets = /* @__PURE__ */ new Map();
  for (const c of allColors) {
    const px = rgb(c.hex);
    const ch = chroma(px);
    const l = lum(px);
    if (ch < 0.3 || l < 0.03 || l > 0.95) continue;
    const bucket = Math.round(hue(px) / 30) % 12;
    const score = ch * Math.sqrt(Math.max(1, c.w));
    const entry = buckets.get(bucket) ?? { score: 0, best: "", bestScore: 0 };
    entry.score += score;
    if (score > entry.bestScore) {
      entry.bestScore = score;
      entry.best = c.hex;
    }
    buckets.set(bucket, entry);
  }
  let accent = "";
  let bestBucketScore = 0;
  for (const entry of buckets.values()) {
    if (entry.score > bestBucketScore) {
      bestBucketScore = entry.score;
      accent = entry.best;
    }
  }
  if (!accent) accent = dark ? "#6E7BFF" : "#3B5BFF";
  const onPrimary = contrast(white, accent) >= contrast(black, accent) ? "#FFFFFF" : "#0A0A0B";
  return { bg: bgHex, surface, elevated, text, muted, primary: accent, onPrimary, border, accent };
}

// src/brand/capture.ts
var DEFAULTS = {
  width: 1440,
  height: 900,
  maxHeight: 3200,
  settleMs: 1800
};
async function safeEvaluate(b, expression) {
  try {
    await evaluate(b, expression);
  } catch {
  }
}
async function dismissConsentOverlays(b) {
  await safeEvaluate(
    b,
    `(function () {
      try {
        var attrs = ["id", "class", "aria-label"];
        var tags = ["button", "a", '[role="button"]'];
        var selectors = [];
        for (var t = 0; t < tags.length; t++) {
          for (var a = 0; a < attrs.length; a++) {
            selectors.push(tags[t] + '[' + attrs[a] + '*="accept" i]');
          }
        }
        var seen = new Set();
        var els = document.querySelectorAll(selectors.join(","));
        for (var i = 0; i < els.length; i++) {
          var el = els[i];
          if (seen.has(el)) continue;
          seen.add(el);
          try { el.click(); } catch (e) { /* ignore */ }
        }
      } catch (e) { /* ignore \u2014 best-effort only */ }
      return true;
    })()`
  );
}
async function triggerLazyLoad(b) {
  await safeEvaluate(
    b,
    `(async function () {
      try {
        var step = Math.max(200, Math.floor(window.innerHeight * 0.8));
        var y = 0;
        var h = document.documentElement.scrollHeight;
        var guard = 0;
        while (y < h && guard < 60) {
          window.scrollTo(0, y);
          await new Promise(function (r) { setTimeout(r, 120); });
          y += step;
          h = document.documentElement.scrollHeight;
          guard++;
        }
        window.scrollTo(0, 0);
        await new Promise(function (r) { setTimeout(r, 150); });
      } catch (e) { /* ignore \u2014 best-effort only */ }
      return true;
    })()`
  );
}
async function waitForFonts(b) {
  await safeEvaluate(
    b,
    `(async function () {
      try {
        if (document.fonts && document.fonts.ready) await document.fonts.ready;
      } catch (e) { /* ignore \u2014 best-effort only */ }
      return true;
    })()`
  );
}
async function captureInBrowser(b, url, opts = {}) {
  const o = { ...DEFAULTS, ...opts };
  await b.send("Emulation.setDeviceMetricsOverride", {
    width: o.width,
    height: o.height,
    deviceScaleFactor: 1,
    mobile: false
  });
  await navigate(b, url, o.settleMs);
  await dismissConsentOverlays(b);
  await triggerLazyLoad(b);
  await waitForFonts(b);
  let contentH = o.height;
  try {
    const metrics = await b.send("Page.getLayoutMetrics");
    const h = metrics.cssContentSize?.height;
    if (typeof h === "number" && h > 0) contentH = Math.min(Math.ceil(h), o.maxHeight);
  } catch {
  }
  let palette;
  try {
    palette = await extractRawPalette(b);
  } catch {
  }
  const shot = await b.send("Page.captureScreenshot", {
    format: "png",
    captureBeyondViewport: true,
    clip: { x: 0, y: 0, width: o.width, height: contentH, scale: 1 }
  });
  return { url, png: Buffer.from(shot.data, "base64"), width: o.width, height: contentH, palette };
}
async function captureUrls(urls, opts = {}) {
  const chromePath = opts.chromePath ?? findChrome();
  if (!chromePath) throw new Error("No Chrome found \u2014 set PLUMB_CHROME=/path/to/chrome.");
  const browser = await launchBrowser({ chromePath });
  const captures = [];
  const misses = [];
  try {
    for (const url of urls) {
      try {
        captures.push(await captureInBrowser(browser, url, opts));
      } catch (e) {
        misses.push({ url, error: e instanceof Error ? e.message : String(e) });
      }
    }
  } finally {
    await browser.close();
  }
  return { captures, misses };
}

// src/brand/board.ts
var SWATCH_ROLES = ["bg", "surface", "text", "muted", "primary", "border"];
function buildBrandFoundations(brand, type, fonts) {
  const collection = {
    name: "Brand",
    modes: ["Value"],
    variables: Object.keys(brand).map((role) => ({
      name: `color/${role}`,
      type: "COLOR",
      values: { Value: { hex: brand[role] } }
    }))
  };
  const resolveFont = (font) => {
    if (font === "heading") return fonts.heading ?? "Inter";
    if (font === "body") return fonts.body ?? "Inter";
    if (font === "mono") return fonts.mono ?? "Inter";
    return font ?? fonts.body ?? "Inter";
  };
  const textStyles = Object.entries(type).filter((entry) => !!entry[1]).map(([name, style]) => ({
    name: `Brand/${name.charAt(0).toUpperCase()}${name.slice(1)}`,
    family: resolveFont(style.font),
    weight: style.weight ?? 400,
    sizePx: style.size,
    lineHeightPercent: style.line ? Math.round(style.line * 100) : void 0,
    letterSpacingPx: style.tracking,
    textCase: style.transform === "upper" ? "UPPER" : "ORIGINAL"
  }));
  return { collections: [collection], textStyles, effectStyles: [], gridStyles: [] };
}
function buildBrandBoard(input) {
  const { brief, references, captures, brand } = input;
  const assets = {};
  const keyByUrl = /* @__PURE__ */ new Map();
  for (const cap of captures) {
    const key = stageInboundAsset(cap.png, "png");
    assets[key] = { ext: "png", kind: "image", w: cap.width, h: cap.height };
    keyByUrl.set(cap.url, { key, w: cap.width, h: cap.height });
  }
  const resolver = {
    async resolve(spec) {
      const hit = keyByUrl.get(spec.query);
      if (hit) {
        return {
          assetId: hit.key,
          kind: "photo",
          w: hit.w,
          h: hit.h,
          scaleMode: "fill"
        };
      }
      return { kind: spec.kind ?? "photo" };
    }
  };
  const referenceRow = {
    type: "stack",
    name: "references",
    dir: "row",
    gap: 20,
    w: "fill",
    children: references.map(
      (ref) => ({
        type: "stack",
        name: "reference-card",
        dir: "col",
        grow: 1,
        gap: 10,
        children: [
          {
            type: "image",
            src: { query: ref.url, kind: "photo" },
            w: "fill",
            h: 230,
            radius: 10,
            fit: "crop"
          },
          { type: "text", text: ref.name, style: "h3" },
          { type: "text", text: ref.why, style: "small", color: "@muted", w: "fill" }
        ]
      })
    )
  };
  const swatchRow = {
    type: "stack",
    name: "swatches",
    dir: "row",
    gap: 16,
    w: "fill",
    children: SWATCH_ROLES.map((role) => {
      const hex = brand[role];
      return {
        type: "stack",
        name: `swatch-${role}`,
        dir: "col",
        grow: 1,
        gap: 8,
        children: [
          {
            type: "stack",
            name: "chip",
            dir: "col",
            w: "fill",
            h: 96,
            bg: hex,
            radius: 10,
            border: { color: "@border", width: 1 },
            children: []
          },
          { type: "text", text: role, style: "label" },
          { type: "text", text: hex, style: "small", color: "@muted" }
        ]
      };
    })
  };
  const typographySamples = [
    { type: "text", text: "Display 64", style: "display" },
    { type: "text", text: "Heading 40", style: "h1" },
    {
      type: "text",
      text: "Body copy sets the reading rhythm for the whole system \u2014 comfortable size, generous line-height, and a muted tone that stays legible without competing with headings. This paragraph shows the body style wrapping within its column.",
      style: "body",
      color: "@muted",
      w: "fill"
    }
  ];
  const fonts = { heading: "Inter", body: "Inter" };
  const type = {
    display: { size: 64, weight: 600, line: 1.05, font: "heading" },
    h1: { size: 40, weight: 600, line: 1.1, font: "heading" },
    h2: { size: 28, weight: 600, line: 1.2, font: "heading" },
    h3: { size: 20, weight: 600, line: 1.3, font: "heading" },
    body: { size: 16, weight: 400, line: 1.6, font: "body" },
    small: { size: 13, weight: 400, line: 1.5, font: "body" },
    label: { size: 12, weight: 600, line: 1.2, font: "body", transform: "upper" }
  };
  const doc = {
    version: "1",
    meta: {
      name: brief.name ?? "Brand board",
      description: brief.description
    },
    brand: {
      fonts,
      // palette.BrandColors is a fully-concrete role map; the DSL's BrandColors
      // carries an index signature, so bridge it explicitly.
      colors: brand,
      type,
      spacing: { xs: 4, sm: 8, md: 16, lg: 24, xl: 48 },
      radius: { sm: 8, md: 12, lg: 16, full: "full" }
    },
    pages: [
      {
        name: "Brand",
        width: 1440,
        bg: "@bg",
        sections: [
          {
            role: "content",
            children: [{ type: "text", text: "References", style: "h2" }, referenceRow]
          },
          {
            role: "content",
            children: [{ type: "text", text: "Brand colours", style: "h2" }, swatchRow]
          },
          {
            role: "content",
            children: [{ type: "text", text: "Typography", style: "h2" }, ...typographySamples]
          }
        ]
      }
    ]
  };
  const foundations = buildBrandFoundations(brand, type, fonts);
  return { doc, resolver, assets, foundations };
}

// src/tools/brand.ts
var DESCRIPTION2 = 'Direct a brand board from a one-line brief and BUILD it into the paired Figma file. Given a brief (e.g. "a premium fintech dashboard"), Plumb discovers a diverse set of best-in-class reference sites, screenshots them live, synthesizes a coherent semantic palette from their computed CSS, and assembles a single 1440-wide Brand page (reference screenshots + colour swatches + a type scale) \u2014 then compiles it down through the same DSL \u2192 PDS \u2192 emit-plan write path plumb_design uses and executes it via the plugin. Returns the picked references, the synthesized brand palette, and the built root node id. Requires the Plumb plugin to be paired.';
function toEmitTarget(t) {
  return { kind: "page", pageName: t.pageName };
}
function registerBrandTool(server) {
  server.registerTool(
    "plumb_brand",
    {
      title: "Plumb \xB7 brand (direct + build)",
      description: DESCRIPTION2,
      inputSchema: {
        brief: z2.string().min(1).describe(
          `One-line description of what's being built, e.g. "a premium fintech dashboard". Drives reference discovery + palette.`
        ),
        count: z2.number().int().positive().optional().describe("How many reference sites to study + screenshot (default 4)."),
        references: z2.array(z2.string()).optional().describe(
          "Explicit reference URLs to include first \u2014 always studied ahead of the discovered catalogue."
        )
      },
      annotations: { readOnlyHint: false, idempotentHint: false, openWorldHint: false }
    },
    async (args) => {
      try {
        const brief = typeof args.brief === "string" ? args.brief.trim() : "";
        if (!brief) {
          throw new PlumbError(
            "A brief is required.",
            'Pass `brief` \u2014 a one-line description like "a premium fintech dashboard".'
          );
        }
        const count = args.count && args.count > 0 ? Math.floor(args.count) : 4;
        const references = Array.isArray(args.references) ? args.references : void 0;
        const refs = discoverReferences({ description: brief, references }, count);
        if (refs.length === 0) {
          throw new PlumbError(
            "Could not resolve any references for the brief.",
            "Add explicit `references` URLs or broaden the brief wording, then retry."
          );
        }
        if (!bridge.paired) {
          throw new PlumbError(
            "No Figma plugin is paired.",
            "plumb_brand writes through the plugin path \u2014 run the Plumb plugin in Figma and click 'Pair with Plumb'."
          );
        }
        emitStudio({
          kind: "log",
          tool: "plumb_brand",
          screen: "Brand",
          summary: `capturing ${refs.length} reference site(s)\u2026`
        });
        const { captures, misses } = await captureUrls(
          refs.map((r) => r.url),
          { maxHeight: 2200, settleMs: 1600 }
        );
        for (const cap of captures) {
          emitStudio({
            kind: "log",
            tool: "plumb_brand",
            screen: "Brand",
            summary: `captured ${cap.url} (${cap.width}\xD7${cap.height})`
          });
        }
        for (const miss of misses) {
          emitStudio({
            kind: "log",
            tool: "plumb_brand",
            screen: "Brand",
            summary: `missed ${miss.url} \u2014 ${miss.error}`
          });
        }
        const rawPalettes = captures.map((c) => c.palette).filter((p) => !!p);
        if (rawPalettes.length === 0) {
          throw new PlumbError(
            "No reference screenshot yielded a palette to synthesize a brand from.",
            "Check network access + that Chrome can reach the reference sites (PLUMB_CHROME), then retry."
          );
        }
        const brand = synthesizeBrandFromMany(rawPalettes);
        const { doc, resolver, assets, foundations } = buildBrandBoard({
          brief: { description: brief },
          references: refs,
          captures,
          brand
        });
        let foundationsWarnings = [];
        try {
          const foundationsResult = await requestApplyFoundations(foundations);
          foundationsWarnings = foundationsResult.warnings;
          emitStudio({
            kind: "log",
            tool: "plumb_brand",
            screen: "Brand",
            summary: `foundations: ${foundations.collections[0]?.variables.length ?? 0} colour token(s), ${foundations.textStyles.length} text style(s)` + (foundationsWarnings.length ? ` \u2014 ${foundationsWarnings.length} warning(s)` : "")
          });
        } catch (e) {
          foundationsWarnings = [`foundations apply failed: ${e instanceof Error ? e.message : String(e)}`];
          emitStudio({
            kind: "log",
            tool: "plumb_brand",
            screen: "Brand",
            summary: `foundations skipped \u2014 ${foundationsWarnings[0]}`
          });
        }
        let compiled;
        try {
          compiled = await compile(doc, { assets: resolver });
        } catch (e) {
          throw new PlumbError(
            `Compiling the brand board failed: ${e instanceof Error ? e.message : String(e)}`,
            "This is usually a malformed section/block. Retry, or simplify the brief."
          );
        }
        const planId = `brand-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const plan = lowerToEmitPlan(compiled.doc, {
          planId,
          target: toEmitTarget({ kind: "page", pageName: "Brand" }),
          mode: "create",
          prune: false,
          reveal: true,
          components: compiled.components,
          assets
        });
        const { result, error } = await requestApplyDesign(
          plan,
          (p) => {
            emitStudio({
              kind: "log",
              tool: "plumb_brand",
              screen: "Brand",
              summary: `${p.phase} ${p.done}/${p.total}${p.note ? ` \u2014 ${p.note}` : ""}`
            });
          }
        );
        if (error || !result) {
          throw new PlumbError(
            `The plugin could not build the brand board: ${error ?? "no result returned"}`,
            "Check that the plugin is still paired, then retry."
          );
        }
        emitStudio({
          kind: "screen",
          tool: "plumb_brand",
          screen: "Brand",
          summary: `built brand board \u2014 ${captures.length} reference(s), ${result.created} node(s)`
        });
        return ok({
          references: refs.map((r) => ({ name: r.name, url: r.url })),
          captured: captures.length,
          misses,
          brand,
          rootId: result.rootId,
          foundationsWarnings,
          summary: "built brand board"
        });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/components.ts
import { z as z3 } from "zod";
var DESCRIPTION3 = "List every Figma component definition in the file and the instance usages of each \u2014 useful for understanding the design system before building. Each component carries an instance count; each instance carries the component id, so you can match usage to definition. Plugin path; needs the Plumb plugin paired.";
function registerPlumbComponents(server) {
  server.registerTool(
    "plumb_components",
    {
      title: "Plumb \xB7 components",
      description: DESCRIPTION3,
      inputSchema: {
        page: z3.string().optional().describe(
          "Filter components and instances to a single Figma page by name (case-insensitive, substring-friendly). Massive token saver on files that hide a 200-variant style guide on one page."
        )
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false }
    },
    async (args) => {
      try {
        if (!bridge.paired) {
          throw new PlumbError(
            "No Figma plugin is paired.",
            "plumb_components uses the plugin path \u2014 run the Plumb plugin in Figma and click 'Pair with Plumb'."
          );
        }
        const { components, instances, error } = await requestComponents();
        if (error) {
          throw new PlumbError(
            `The plugin could not gather components: ${error}`,
            "Retry; if it persists, re-run the Plumb plugin in Figma."
          );
        }
        const pageFilter = args.page?.trim().toLowerCase();
        const filterFn = pageFilter ? (x) => (x.page ?? "").toLowerCase().includes(pageFilter) : () => true;
        const filteredComponents = components.filter(filterFn);
        const filteredInstances = instances.filter(filterFn);
        return ok({
          source: "plugin",
          ...pageFilter ? { page: args.page } : {},
          componentCount: filteredComponents.length,
          instanceCount: filteredInstances.length,
          components: filteredComponents,
          instances: filteredInstances,
          next: filteredComponents.length === 0 ? "No components defined" + (pageFilter ? ` on the "${args.page}" page.` : " in this file.") : "Each instance has a componentId \u2014 match it to a definition. Use plumb_node by id to inspect either."
        });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/describe.ts
import { z as z4 } from "zod";

// src/cache.ts
import { createHash } from "crypto";
import { mkdirSync as mkdirSync3, readFileSync as readFileSync6, writeFileSync as writeFileSync3 } from "fs";
import { homedir as homedir2 } from "os";
import { join as join8 } from "path";
var DEFAULT_TTL_MS = Number(process.env.PLUMB_CACHE_TTL_MS) || 5 * 6e4;
function cacheDir() {
  const base = process.env.PLUMB_CACHE_DIR ?? join8(homedir2(), ".cache", "plumb");
  return join8(base, "v1");
}
function pathFor(key) {
  const hash = createHash("sha1").update(key).digest("hex").slice(0, 20);
  return join8(cacheDir(), `${hash}.json`);
}
function cacheGet(key, maxAgeMs) {
  try {
    const entry = JSON.parse(readFileSync6(pathFor(key), "utf8"));
    if (Date.now() - entry.savedAt > maxAgeMs) return void 0;
    return entry;
  } catch {
    return void 0;
  }
}
function cacheSet(key, version, payload) {
  try {
    mkdirSync3(cacheDir(), { recursive: true });
    const entry = { version, savedAt: Date.now(), payload };
    writeFileSync3(pathFor(key), JSON.stringify(entry));
  } catch {
  }
}

// src/describe.ts
function regionOf(parent, pos, box) {
  const cx = pos.x + box.w / 2;
  const cy = pos.y + box.h / 2;
  const xb = (zone) => zone < parent.w / 3 ? "left" : zone > parent.w * 2 / 3 ? "right" : "middle";
  const yb = (zone) => zone < parent.h / 3 ? "top" : zone > parent.h * 2 / 3 ? "bottom" : "middle";
  const x = xb(cx);
  const y = yb(cy);
  if (y === "top" && x === "left") return "top-left";
  if (y === "top" && x === "right") return "top-right";
  if (y === "top") return "top";
  if (y === "bottom" && x === "left") return "bottom-left";
  if (y === "bottom" && x === "right") return "bottom-right";
  if (y === "bottom") return "bottom";
  if (x === "left") return "left";
  if (x === "right") return "right";
  return "center";
}
function resolveFill(node, tokens) {
  const f = node.fill;
  if (!f) return void 0;
  if (f === "gradient" || f === "image") return f;
  if (f.startsWith("$c")) return tokens.color[f];
  return f;
}
function resolveRadius2(node, tokens) {
  const r = node.radius;
  if (r === void 0) return void 0;
  if (Array.isArray(r)) return `[${r.join(",")}]`;
  if (typeof r === "string" && r.startsWith("$r")) {
    const v = tokens.radius[r];
    if (v === void 0) return void 0;
    return v === "full" ? "full" : `${v}px`;
  }
  return String(r);
}
function summarizeFills(node, tokens) {
  const fills = resolveFills(node.fills, tokens);
  if (fills && fills.length) {
    const parts = fills.map((f) => {
      if (f.type === "color") return f.color;
      if (f.type === "image") return f.assetId ? `image:${f.assetId}` : "image";
      const stops = f.stops.map((s) => s.color).join("\u2192");
      const angle = "angle" in f && f.angle !== void 0 ? `${f.angle}\xB0 ` : "";
      return `${f.type}(${angle}${stops})`;
    });
    return parts.length === 1 ? parts[0] : `[${parts.join(" over ")}]`;
  }
  const fill = resolveFill(node, tokens);
  return fill;
}
function summarizeAppearance(node, tokens) {
  const parts = [];
  const fillSummary = summarizeFills(node, tokens);
  if (fillSummary) parts.push(`fill ${fillSummary}`);
  if (node.stroke && node.strokeW) {
    const stroke = node.stroke.startsWith("$c") ? tokens.color[node.stroke] : node.stroke;
    parts.push(`${node.strokeW}px ${stroke ?? "solid"} border`);
  }
  const radius = resolveRadius2(node, tokens);
  if (radius) parts.push(`radius ${radius}`);
  const effects = resolveEffects(node.effects, tokens);
  if (effects && effects.length) {
    const efParts = effects.map((e) => {
      if (e.type === "background-blur") return `frosted glass (backdrop-blur ${e.radius}px)`;
      if (e.type === "layer-blur") return `blur ${e.radius}px`;
      if (e.type === "inner-shadow") return `inset highlight ${e.color}`;
      if (e.type === "drop-shadow") return `shadow ${e.color}`;
      return "effect";
    });
    parts.push(efParts.join(", "));
  } else if (node.shadow) {
    if (node.shadow.startsWith("$s")) parts.push(`shadow ${tokens.shadow[node.shadow] ?? "yes"}`);
    else parts.push("shadow");
  }
  if (node.backdropFilter) parts.push(`backdrop ${node.backdropFilter}`);
  if (node.textDecoration) parts.push(node.textDecoration);
  if (node.opacity !== void 0 && node.opacity < 1) parts.push(`opacity ${node.opacity}`);
  if (node.clip) parts.push("clipped");
  const layout = resolveLayout(node.layout, tokens);
  if (layout) {
    const dir = layout.flow === "col" ? "column" : "row";
    const gap = layout.gap ? `, gap ${layout.gap}` : "";
    parts.push(`${dir} stack${gap}`);
  }
  if (node.motion && node.motion.length) {
    parts.push(`motion (${node.motion.map((m) => m.trigger).join(", ")})`);
  }
  return parts.length ? parts.join(", ") : "no fill";
}
function nodeWord(t) {
  switch (t) {
    case "frame":
    case "component":
    case "instance":
      return "frame";
    case "rect":
      return "rectangle";
    case "ellipse":
      return "ellipse";
    case "text":
      return "text";
    case "vector":
    case "line":
      return "vector";
    case "group":
      return "group";
    default:
      return t || "node";
  }
}
function describePds(doc) {
  const root = doc.nodes[doc.root];
  if (!root) {
    return {
      root: doc.root,
      box: { w: 0, h: 0 },
      layout: "free",
      narrative: "(root node missing)",
      children: []
    };
  }
  const rootLayout = resolveLayout(root.layout, doc.tokens);
  const hasAutoLayout = !!rootLayout;
  const kids = (root.children ?? []).map((el) => doc.nodes[el]).filter((n) => !!n);
  const summaries = kids.map((k) => {
    const summary = {
      el: k.el,
      id: k.id,
      name: k.name,
      type: k.type,
      box: k.box,
      appearance: summarizeAppearance(k, doc.tokens)
    };
    if (k.pos) summary.pos = k.pos;
    if (!hasAutoLayout && k.pos) {
      summary.region = regionOf(root.box, k.pos, k.box);
    }
    if (k.chars) {
      summary.chars = typeof k.chars === "string" ? k.chars : k.chars.map((r) => r.t).join("");
    }
    return summary;
  });
  const regions = hasAutoLayout ? void 0 : {};
  if (regions) {
    for (const s of summaries) {
      if (!s.region) continue;
      (regions[s.region] ??= []).push(s.el);
    }
  }
  const lines = [];
  const rootWord = nodeWord(root.type);
  const header = hasAutoLayout ? `The "${root.name}" ${rootWord} is ${root.box.w}\xD7${root.box.h}px \u2014 a ${rootLayout.flow === "col" ? "vertical" : "horizontal"} auto-layout container` : `The "${root.name}" ${rootWord} is ${root.box.w}\xD7${root.box.h}px \u2014 a free-form (absolutely-positioned) container`;
  lines.push(
    `${header} with ${kids.length} ${kids.length === 1 ? "child" : "children"}.`
  );
  if (kids.length > 0) {
    lines.push("");
    for (const s of summaries) {
      const where = s.region ? `at the ${s.region}` : s.pos ? `at (${s.pos.x},${s.pos.y})` : "in flow order";
      const text = s.chars ? `, text "${s.chars.slice(0, 64)}${s.chars.length > 64 ? "\u2026" : ""}"` : "";
      lines.push(
        `- ${s.name} (${nodeWord(s.type)}, ${s.box.w}\xD7${s.box.h}) ${where}: ${s.appearance}${text}`
      );
    }
  }
  return {
    root: root.el,
    box: root.box,
    layout: hasAutoLayout ? "auto" : "free",
    narrative: lines.join("\n"),
    regions,
    children: summaries
  };
}

// src/tools/describe.ts
var DESCRIPTION4 = "Text-only visual description of a Figma screen or node \u2014 useful when you can't read the rendered screenshot (image-blind harness, sandboxed Read, or token-conscious flows). Returns a per-region narrative ('top-left: ...') and a flat child summary derived from the PDS. Pair with plumb_node for the full structural spec, or pair with plumb_screenshot for the pixel reference.";
function registerPlumbDescribe(server) {
  server.registerTool(
    "plumb_describe",
    {
      title: "Plumb \xB7 describe",
      description: DESCRIPTION4,
      inputSchema: {
        fileKey: z4.string().optional().describe("REST path file key."),
        url: z4.string().optional().describe("Paste a full Figma URL \u2014 fileKey + node-id auto-extracted."),
        id: z4.string().optional().describe("Node id to describe."),
        name: z4.string().optional().describe("Screen name (plugin path)."),
        depth: z4.number().int().min(1).max(6).optional().describe("Levels of the tree to mine for the narrative. Default 2.")
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: true }
    },
    async (args) => {
      try {
        const depth = args.depth ?? 2;
        const { fileKey, id } = resolveFigmaTarget({
          url: args.url,
          fileKey: args.fileKey,
          id: args.id
        });
        if (!fileKey && bridge.paired) {
          const resolved = resolveScreen(id, args.name);
          if ("ambiguous" in resolved) {
            return ok({
              ambiguous: true,
              matches: formatScreenMatches(resolved.ambiguous),
              next: "Several screens share that name. Each row shows `page` and `box` (w\xD7h) \u2014 pick the one you want and re-call plumb_describe with its `id`."
            });
          }
          const { doc, nodeName } = await requestNode(resolved.id);
          if (!doc) {
            throw new PlumbError(
              `The Plumb plugin could not find node "${resolved.id}".`,
              "Call plumb_outline for the current screen list."
            );
          }
          const file2 = {
            document: doc,
            fileName: bridge.inventory?.fileName ?? "",
            version: `plugin-${Date.now()}`
          };
          const pds2 = normalizeToBudget(file2, depth, void 0);
          return ok({ ...describePds(pds2), source: "plugin", screen: nodeName });
        }
        if (!fileKey || !id) {
          throw new PlumbError(
            "plumb_describe needs the Plumb plugin paired, or a fileKey + id (or a Figma url).",
            "Pair the plugin in Figma, paste a Figma URL via `url`, or pass both fileKey and id."
          );
        }
        const cacheKey = `describe:${fileKey}:${id}:${depth}`;
        const hit = cacheGet(cacheKey, DEFAULT_TTL_MS);
        if (hit) return ok({ ...describePds(hit.payload), source: "rest", cached: true });
        const token = requireToken2();
        const file = await fetchNodeViaRest({
          fileKey,
          nodeId: id,
          depth: depth + 1,
          token
        });
        const pds = normalizeToBudget(file, depth, void 0);
        cacheSet(cacheKey, file.version, pds);
        return ok({ ...describePds(pds), source: "rest", cached: false });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/design.ts
import { writeFileSync as writeFileSync4 } from "fs";
import { tmpdir as tmpdir3 } from "os";
import { join as join9 } from "path";
import { z as z6 } from "zod";

// src/dsl/schema.ts
import { z as z5 } from "zod";
var GradientSchema = z5.object({
  kind: z5.enum(["linear", "radial", "angular", "diamond"]),
  angle: z5.number().optional(),
  stops: z5.array(z5.object({ at: z5.number(), color: z5.string() }))
});
var ColorSchema = z5.union([z5.string(), GradientSchema]);
var SpaceSchema = z5.union([z5.number(), z5.string()]);
var RadiusTokSchema = z5.union([z5.number(), z5.literal("full"), z5.string()]);
var SizeSchema = z5.union([z5.literal("hug"), z5.literal("fill"), z5.number(), z5.string()]);
var JustifySchema = z5.enum(["start", "center", "end", "between"]);
var AlignSchema = z5.enum(["start", "center", "end", "stretch", "baseline"]);
var SelfAlignSchema = z5.enum(["start", "center", "end", "stretch"]);
var PadSpecSchema = z5.union([
  SpaceSchema,
  z5.tuple([SpaceSchema, SpaceSchema]),
  z5.tuple([SpaceSchema, SpaceSchema, SpaceSchema, SpaceSchema])
]);
var AssetKindSchema = z5.enum([
  "icon",
  "photo",
  "illustration",
  "avatar",
  "pattern",
  "font",
  "mockup",
  "logo",
  "generated"
]);
var StyleTagSchema = z5.enum([
  "flat",
  "line",
  "outline",
  "filled",
  "duotone",
  "3d",
  "photo",
  "illustration",
  "hand-drawn",
  "geometric",
  "gradient"
]);
var IconWeightSchema = z5.enum(["thin", "light", "regular", "medium", "bold"]);
var AssetSpecSchema = z5.object({
  query: z5.string(),
  kind: AssetKindSchema.optional(),
  role: z5.enum([
    "hero",
    "nav-logo",
    "card-image",
    "avatar",
    "icon",
    "background",
    "section-illustration",
    "mockup",
    "brand-logo"
  ]).optional(),
  style: z5.array(StyleTagSchema).optional(),
  weight: IconWeightSchema.optional(),
  aspect: z5.number().optional(),
  minWidth: z5.number().optional(),
  palette: z5.array(z5.string()).optional(),
  seed: z5.string().optional(),
  provider: z5.string().optional(),
  w: z5.number().optional(),
  h: z5.number().optional()
});
var AssetSrcSchema = z5.union([AssetSpecSchema, z5.string()]);
var TypeStyleSchema = z5.object({
  size: z5.number(),
  weight: z5.number().optional(),
  line: z5.number().optional(),
  tracking: z5.number().optional(),
  font: z5.string().optional(),
  color: ColorSchema.optional(),
  transform: z5.enum(["upper", "lower", "none"]).optional(),
  decoration: z5.enum(["underline", "line-through"]).optional()
});
var ShadowSpecSchema = z5.object({
  x: z5.number(),
  y: z5.number(),
  blur: z5.number(),
  spread: z5.number(),
  color: z5.string(),
  inset: z5.boolean().optional()
});
var BrandSchema = z5.object({
  fonts: z5.object({
    heading: z5.string().optional(),
    body: z5.string().optional(),
    mono: z5.string().optional()
  }).optional(),
  colors: z5.object({
    bg: ColorSchema,
    text: ColorSchema,
    primary: ColorSchema
  }).catchall(ColorSchema.optional()),
  type: z5.record(z5.string(), TypeStyleSchema.optional()),
  spacing: z5.record(z5.string(), z5.number().optional()).optional(),
  radius: z5.record(z5.string(), z5.union([z5.number(), z5.literal("full")]).optional()).optional(),
  shadow: z5.record(z5.string(), ShadowSpecSchema.optional()).optional()
});
var DslTriggerSchema = z5.string().refine(
  (s) => [
    "click",
    "hover",
    "press",
    "drag",
    "mouse-enter",
    "mouse-leave",
    "mouse-down",
    "mouse-up"
  ].includes(s) || /^key:.+$/.test(s) || /^after:\d+$/.test(s),
  { message: "invalid trigger" }
);
var DslEasingSchema = z5.union([
  z5.enum([
    "linear",
    "ease-in",
    "ease-out",
    "ease-in-out",
    "ease-in-back",
    "ease-out-back",
    "ease-in-out-back",
    "gentle",
    "quick",
    "bouncy",
    "slow"
  ]),
  z5.object({ bezier: z5.tuple([z5.number(), z5.number(), z5.number(), z5.number()]) }),
  z5.object({
    spring: z5.object({ mass: z5.number(), stiffness: z5.number(), damping: z5.number() })
  })
]);
var DslTransitionSchema = z5.object({
  kind: z5.enum([
    "smart",
    "dissolve",
    "move-in",
    "move-out",
    "push",
    "slide-in",
    "slide-out",
    "scroll-animate",
    "instant"
  ]),
  from: z5.enum(["left", "right", "top", "bottom"]).optional(),
  duration: z5.number().optional(),
  ease: DslEasingSchema.optional(),
  matchLayers: z5.boolean().optional()
});
var DslInteractionSchema = z5.object({
  on: DslTriggerSchema,
  go: z5.string().optional(),
  swap: z5.string().optional(),
  overlay: z5.string().optional(),
  scrollTo: z5.string().optional(),
  back: z5.boolean().optional(),
  close: z5.boolean().optional(),
  url: z5.string().optional(),
  set: z5.record(z5.string(), z5.union([z5.string(), z5.number(), z5.boolean()])).optional(),
  animate: DslTransitionSchema.optional(),
  keepScroll: z5.boolean().optional(),
  resetState: z5.boolean().optional()
});
var DslOverlayConfigSchema = z5.object({
  position: z5.enum([
    "center",
    "top",
    "bottom",
    "left",
    "right",
    "top-left",
    "top-right",
    "bottom-left",
    "bottom-right",
    "manual"
  ]).optional(),
  at: z5.object({ x: z5.number(), y: z5.number() }).optional(),
  backdrop: z5.string().optional(),
  closeOnClickOutside: z5.boolean().optional()
});
var DslPrototypeSchema = z5.object({
  device: z5.union([
    z5.literal("none"),
    z5.object({ preset: z5.string() }),
    z5.object({ size: z5.object({ w: z5.number(), h: z5.number() }) })
  ]).optional(),
  rotation: z5.enum(["portrait", "landscape"]).optional(),
  background: z5.string().optional(),
  start: z5.string().optional()
});
var TextRunSchema = z5.object({
  t: z5.string(),
  style: z5.string().optional(),
  color: ColorSchema.optional(),
  decoration: z5.enum(["underline", "line-through"]).optional()
});
var TextValueSchema = z5.union([z5.string(), z5.array(TextRunSchema)]);
var BorderSpecSchema = z5.object({
  color: ColorSchema,
  width: z5.number(),
  align: z5.enum(["inside", "outside", "center"]).optional(),
  dash: z5.array(z5.number()).optional()
});
var blockBaseShape = {
  id: z5.string().optional(),
  name: z5.string().optional(),
  w: SizeSchema.optional(),
  h: SizeSchema.optional(),
  grow: z5.number().optional(),
  self: SelfAlignSchema.optional(),
  bg: ColorSchema.optional(),
  border: BorderSpecSchema.optional(),
  radius: RadiusTokSchema.optional(),
  shadow: z5.string().optional(),
  opacity: z5.number().optional(),
  pad: PadSpecSchema.optional(),
  clip: z5.boolean().optional(),
  pos: z5.object({ x: z5.number(), y: z5.number() }).optional(),
  blend: z5.enum([
    "normal",
    "multiply",
    "screen",
    "overlay",
    "darken",
    "lighten",
    "color-dodge",
    "color-burn",
    "hard-light",
    "soft-light",
    "difference",
    "exclusion",
    "hue",
    "saturation",
    "color",
    "luminosity"
  ]).optional(),
  blur: z5.number().optional(),
  interactions: z5.array(DslInteractionSchema).optional()
};
var BlockSchema;
var lazyBlock = z5.lazy(() => BlockSchema);
var lazyBlockArray = z5.array(lazyBlock);
var StackSchema = z5.object({
  ...blockBaseShape,
  type: z5.literal("stack"),
  dir: z5.enum(["row", "col"]).optional(),
  gap: SpaceSchema.optional(),
  gapCross: SpaceSchema.optional(),
  justify: JustifySchema.optional(),
  align: AlignSchema.optional(),
  wrap: z5.boolean().optional(),
  children: lazyBlockArray
});
var TextSchema = z5.object({
  ...blockBaseShape,
  type: z5.literal("text"),
  text: TextValueSchema,
  style: z5.union([z5.string(), TypeStyleSchema]).optional(),
  color: ColorSchema.optional(),
  textAlign: z5.enum(["left", "center", "right", "justify"]).optional(),
  maxLines: z5.number().optional(),
  resize: z5.enum(["h", "wh", "trunc"]).optional()
});
var ImageSchema = z5.object({
  ...blockBaseShape,
  type: z5.literal("image"),
  src: AssetSrcSchema,
  fit: z5.enum(["fill", "fit", "stretch", "crop", "tile"]).optional(),
  alt: z5.string().optional()
});
var IconSchema = z5.object({
  ...blockBaseShape,
  type: z5.literal("icon"),
  name: z5.string(),
  size: z5.number().optional(),
  color: ColorSchema.optional()
});
var ButtonSchema = z5.object({
  ...blockBaseShape,
  type: z5.literal("button"),
  label: z5.string(),
  variant: z5.enum(["primary", "secondary", "outline", "ghost", "link"]).optional(),
  size: z5.enum(["sm", "md", "lg"]).optional(),
  icon: z5.string().optional(),
  iconPos: z5.enum(["left", "right"]).optional(),
  href: z5.string().optional()
});
var FieldSchema = z5.object({
  ...blockBaseShape,
  type: z5.literal("field"),
  kind: z5.enum(["text", "email", "password", "textarea", "select", "checkbox"]).optional(),
  label: z5.string().optional(),
  placeholder: z5.string().optional()
});
var InstanceSchema = z5.object({
  ...blockBaseShape,
  type: z5.literal("instance"),
  component: z5.string(),
  props: z5.record(z5.string(), z5.union([z5.string(), z5.number(), z5.boolean()])).optional(),
  slots: z5.record(z5.string(), lazyBlockArray).optional()
});
var SlotSchema = z5.object({
  ...blockBaseShape,
  type: z5.literal("slot"),
  name: z5.string()
});
var SpacerSchema = z5.object({
  ...blockBaseShape,
  type: z5.literal("spacer"),
  size: SpaceSchema.optional()
});
var DividerSchema = z5.object({
  ...blockBaseShape,
  type: z5.literal("divider")
});
BlockSchema = z5.discriminatedUnion("type", [
  StackSchema,
  TextSchema,
  ImageSchema,
  IconSchema,
  ButtonSchema,
  FieldSchema,
  InstanceSchema,
  SlotSchema,
  SpacerSchema,
  DividerSchema
]);
var PropDefSchema = z5.object({
  name: z5.string(),
  type: z5.enum(["text", "number", "boolean", "color", "enum"]),
  default: z5.union([z5.string(), z5.number(), z5.boolean()]).optional(),
  options: z5.array(z5.string()).optional()
});
var ComponentSchema = z5.object({
  name: z5.string(),
  props: z5.array(PropDefSchema).optional(),
  slots: z5.array(z5.string()).optional(),
  body: lazyBlock
});
var LinkSchema = z5.object({
  label: z5.string(),
  href: z5.string().optional()
});
var CardSchema = z5.object({
  icon: z5.string().optional(),
  image: AssetSrcSchema.optional(),
  title: z5.string(),
  body: z5.string().optional(),
  action: z5.lazy(() => ButtonSchema).optional()
});
var FooterColumnSchema = z5.object({
  title: z5.string(),
  links: z5.array(LinkSchema)
});
var sectionBaseShape = {
  id: z5.string().optional(),
  bg: ColorSchema.optional(),
  pad: PadSpecSchema.optional(),
  gap: SpaceSchema.optional(),
  maxWidth: z5.number().optional(),
  bleed: z5.boolean().optional(),
  align: AlignSchema.optional(),
  justify: JustifySchema.optional(),
  theme: z5.enum(["light", "dark"]).optional(),
  start: z5.union([z5.boolean(), z5.object({ name: z5.string() })]).optional(),
  overlay: DslOverlayConfigSchema.optional(),
  scroll: z5.enum(["none", "vertical", "horizontal", "both"]).optional()
};
var brandBlockSchema = z5.object({ logo: AssetSrcSchema.optional(), text: z5.string().optional() }).optional();
var NavSectionSchema = z5.object({
  ...sectionBaseShape,
  role: z5.literal("nav"),
  brand: brandBlockSchema,
  links: z5.array(LinkSchema),
  actions: z5.array(ButtonSchema).optional(),
  sticky: z5.boolean().optional()
});
var HeroSectionSchema = z5.object({
  ...sectionBaseShape,
  role: z5.literal("hero"),
  eyebrow: z5.string().optional(),
  headline: TextValueSchema,
  sub: z5.string().optional(),
  actions: z5.array(ButtonSchema).optional(),
  media: ImageSchema.optional(),
  layout: z5.enum(["center", "split-left", "split-right"]).optional()
});
var CardGridSectionSchema = z5.object({
  ...sectionBaseShape,
  role: z5.literal("card-grid"),
  heading: z5.string().optional(),
  columns: z5.number().optional(),
  columnRatios: z5.array(z5.number()).optional(),
  cards: z5.array(CardSchema)
});
var FeaturesSectionSchema = z5.object({
  ...sectionBaseShape,
  role: z5.literal("features"),
  heading: z5.string().optional(),
  features: z5.array(CardSchema),
  layout: z5.enum(["row", "alternating"]).optional(),
  columnRatios: z5.array(z5.number()).optional()
});
var FormSectionSchema = z5.object({
  ...sectionBaseShape,
  role: z5.literal("form"),
  heading: z5.string().optional(),
  fields: z5.array(FieldSchema),
  submit: ButtonSchema
});
var CtaSectionSchema = z5.object({
  ...sectionBaseShape,
  role: z5.literal("cta"),
  headline: TextValueSchema,
  sub: z5.string().optional(),
  actions: z5.array(ButtonSchema)
});
var FooterSectionSchema = z5.object({
  ...sectionBaseShape,
  role: z5.literal("footer"),
  brand: brandBlockSchema,
  columns: z5.array(FooterColumnSchema).optional(),
  legal: z5.string().optional()
});
var ContentSectionSchema = z5.object({
  ...sectionBaseShape,
  role: z5.literal("content"),
  children: lazyBlockArray
});
var CustomSectionSchema = z5.object({
  ...sectionBaseShape,
  role: z5.literal("custom"),
  root: lazyBlock
});
var SectionSchema = z5.discriminatedUnion("role", [
  NavSectionSchema,
  HeroSectionSchema,
  CardGridSectionSchema,
  FeaturesSectionSchema,
  FormSectionSchema,
  CtaSectionSchema,
  FooterSectionSchema,
  ContentSectionSchema,
  CustomSectionSchema
]);
var PageSchema = z5.object({
  id: z5.string().optional(),
  name: z5.string(),
  width: z5.number().optional(),
  height: z5.union([z5.number(), z5.literal("auto")]).optional(),
  bg: ColorSchema.optional(),
  sections: z5.array(SectionSchema)
});
var DesignDocSchema = z5.object({
  version: z5.literal("1"),
  meta: z5.object({ name: z5.string().optional(), description: z5.string().optional() }).optional(),
  brand: BrandSchema,
  components: z5.array(ComponentSchema).optional(),
  pages: z5.array(PageSchema),
  prototype: DslPrototypeSchema.optional()
});

// src/tools/design.ts
var DESCRIPTION5 = "Author a design from a high-level Plumb Design DSL document and BUILD it into the paired Figma file. This is the write direction: the DSL (semantic pages \u2192 sections \u2192 blocks + brand tokens) is validated, compiled DOWN to the PDS IR, lowered to a Figma-native emit plan, and executed by the plugin (auto-layout frames, text, images, icons). Returns the created node ids keyed by authored element handle \u2014 the join key you then feed to plumb_review / motion. Pass a `brief` to record intent (audience, tone, brand) alongside the build. Use `dryRun: true` to compile + validate without touching Figma.";
var BriefSchema = z6.object({
  name: z6.string().optional().describe("Product / project name."),
  description: z6.string().optional().describe("One-line description of what is being built."),
  audience: z6.string().optional().describe("Who it's for."),
  tone: z6.array(z6.string()).optional().describe('Voice adjectives, e.g. ["confident","playful"].'),
  industry: z6.string().optional().describe("Domain / vertical."),
  keywords: z6.array(z6.string()).optional().describe("Guiding keywords."),
  references: z6.array(z6.string()).optional().describe("Reference sites / inspirations."),
  brandColors: z6.array(z6.string()).optional().describe("Seed brand hexes, if the caller has them."),
  mustHave: z6.array(z6.string()).optional().describe("Non-negotiables."),
  avoid: z6.array(z6.string()).optional().describe("Things to steer clear of.")
}).strict();
var TargetSchema = z6.object({
  kind: z6.enum(["page", "into", "replace"]).describe(
    "`page` \u2014 new top-level frame on a Figma page; `into` \u2014 append inside an existing node; `replace` \u2014 swap an existing node in place."
  ),
  nodeId: z6.string().optional().describe("Target node id (required for `into` / `replace`)."),
  pageName: z6.string().optional().describe("Figma page to place onto (`page` kind only)."),
  pos: z6.object({ x: z6.number(), y: z6.number() }).optional().describe("Canvas position for the new root (`page` kind only).")
}).strict();
function toResolved(r, assetId) {
  return {
    assetId: assetId ?? r.assetId,
    inlineSvg: r.inlineSvg,
    vectorPath: r.vectorPath,
    url: r.url,
    w: r.w,
    h: r.h,
    kind: r.kind,
    scaleMode: r.scaleMode
  };
}
function makeSourcingResolver() {
  const assets = {};
  const warnings = [];
  const resolver = {
    async resolve(spec) {
      const r = await resolveAsset(spec);
      if (spec.kind === "photo" && (r.placeholder || r.provider === "picsum")) {
        warnings.push(
          `asset "${spec.query ?? spec.kind}": no photo API key set \u2014 used a random placeholder. Set UNSPLASH_ACCESS_KEY / PEXELS_API_KEY / PIXABAY_API_KEY (all free) for on-brief imagery.`
        );
      }
      if (r.inlineSvg) {
        const id = r.assetId ?? `svg${Object.keys(assets).length}`;
        assets[id] = { ext: "svg", kind: "svg", svgInline: r.inlineSvg, w: r.w, h: r.h };
        return toResolved(r, id);
      }
      if (r.bytes && r.bytes.length) {
        const ext = r.ext ?? "png";
        const key = stageInboundAsset(r.bytes, ext);
        assets[key] = { ext, kind: "image", w: r.w, h: r.h };
        return toResolved(r, key);
      }
      return toResolved(r);
    }
  };
  return { resolver, assets, warnings };
}
function formatIssues(err) {
  return err.issues.slice(0, 8).map((i) => {
    const path = i.path.join(".") || "(root)";
    return `${path}: ${i.message}`;
  }).join("; ");
}
function toEmitTarget2(t) {
  if (!t || t.kind === "page") {
    return { kind: "page", pos: t?.pos, pageName: t?.pageName };
  }
  if (!t.nodeId) {
    throw new PlumbError(
      `target.kind "${t.kind}" requires a target.nodeId.`,
      'Pass the Figma node id to build into / replace, or use target.kind "page".'
    );
  }
  return t.kind === "into" ? { kind: "into", nodeId: t.nodeId } : { kind: "replace", nodeId: t.nodeId };
}
function applyBrief(doc, brief) {
  if (!brief) return;
  const meta = doc.meta ?? {};
  if (!meta.name && brief.name) meta.name = brief.name;
  if (!meta.description && brief.description) meta.description = brief.description;
  doc.meta = meta;
}
function registerPlumbDesign(server) {
  server.registerTool(
    "plumb_design",
    {
      title: "Plumb \xB7 design (build)",
      description: DESCRIPTION5,
      inputSchema: {
        doc: z6.unknown().describe(
          'A Plumb Design DSL document (version "1": brand + pages of semantic sections). Validated against DesignDocSchema.'
        ),
        brief: BriefSchema.optional().describe(
          "Optional design-brief intake \u2014 records author intent alongside the build."
        ),
        target: TargetSchema.optional().describe(
          "Where to build. Defaults to a new top-level frame on the current page."
        ),
        mode: z6.enum(["create", "sync"]).optional().describe(
          "`create` (default) \u2014 fresh nodes. `sync` \u2014 idempotent re-apply keyed on stable plumbKey (authored element handle); pair with `prune` to delete stale nodes."
        ),
        prune: z6.boolean().optional().describe("sync only \u2014 delete built nodes no longer in the design."),
        reveal: z6.boolean().optional().describe("Scroll + select the built root in Figma when done."),
        pageWidth: z6.number().int().positive().optional().describe("Layout width hint for pages that don't set their own."),
        dryRun: z6.boolean().optional().describe(
          "Compile + lower only; do NOT touch Figma. Returns plan stats + warnings so you can sanity-check before building."
        )
      },
      annotations: { readOnlyHint: false, idempotentHint: false, openWorldHint: false }
    },
    async (args) => {
      try {
        const dryRun = args.dryRun === true;
        const asObject = (v) => {
          if (typeof v !== "string") return v;
          try {
            return JSON.parse(v);
          } catch {
            return v;
          }
        };
        const parsed = DesignDocSchema.safeParse(asObject(args.doc));
        if (!parsed.success) {
          throw new PlumbError(
            `The design document failed validation: ${formatIssues(parsed.error)}`,
            'Fix the DSL to match DesignDocSchema (version "1", brand, pages[]) and retry.'
          );
        }
        const doc = parsed.data;
        const briefParsed = args.brief ? BriefSchema.safeParse(asObject(args.brief)) : void 0;
        if (briefParsed && !briefParsed.success) {
          throw new PlumbError(
            `The brief failed validation: ${formatIssues(briefParsed.error)}`,
            "Adjust the brief fields (all optional strings / string arrays) and retry."
          );
        }
        const brief = briefParsed?.data;
        applyBrief(doc, brief);
        if (!dryRun && !bridge.paired) {
          throw new PlumbError(
            "No Figma plugin is paired.",
            "plumb_design writes through the plugin path \u2014 run the Plumb plugin in Figma and click 'Pair with Plumb'."
          );
        }
        const sourced = makeSourcingResolver();
        const ctx = {
          assets: sourced.resolver,
          page: args.pageWidth ? { width: args.pageWidth } : void 0
        };
        let compiled;
        try {
          compiled = await compile(doc, ctx);
        } catch (e) {
          throw new PlumbError(
            `Compiling the design failed: ${e instanceof Error ? e.message : String(e)}`,
            "This is usually a malformed section/block. Simplify the offending page and retry."
          );
        }
        const target = toEmitTarget2(args.target);
        const mode = args.mode ?? "create";
        const planId = `plan-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const plan = lowerToEmitPlan(compiled.doc, {
          planId,
          target,
          mode,
          prune: args.prune === true,
          reveal: args.reveal === true,
          components: compiled.components,
          assets: sourced.assets
        });
        const pageCount = doc.pages.length;
        const opCount = plan.ops.length;
        const assetCount = plan.assets?.length ?? 0;
        if (dryRun) {
          emitStudio({
            kind: "log",
            tool: "plumb_design",
            screen: doc.meta?.name ?? null,
            summary: `dry-run compiled ${pageCount} page(s) \u2192 ${opCount} node(s)`
          });
          return ok({
            dryRun: true,
            planId,
            pages: pageCount,
            nodes: opCount,
            assets: assetCount,
            fonts: plan.fonts.length,
            warnings: compiled.warnings,
            next: "Compile succeeded. Re-run without `dryRun` (plugin paired) to build it in Figma."
          });
        }
        const { result, error } = await requestApplyDesign(
          plan,
          (p) => {
            emitStudio({
              kind: "log",
              tool: "plumb_design",
              screen: doc.meta?.name ?? null,
              summary: `${p.phase} ${p.done}/${p.total}${p.note ? ` \u2014 ${p.note}` : ""}`
            });
          }
        );
        if (error || !result) {
          throw new PlumbError(
            `The plugin could not build the design: ${error ?? "no result returned"}`,
            "Check that the plugin is still paired and the target node exists, then retry."
          );
        }
        return finish(doc, compiled, result, planId, sourced.warnings);
      } catch (e) {
        return fail(e);
      }
    }
  );
}
function finish(doc, compiled, result, planId, assetWarnings = []) {
  const warnings = [
    ...new Set(assetWarnings),
    ...compiled.warnings,
    ...result.warnings.map((w) => `${w.key}.${w.field}: ${w.message}`)
  ];
  const summary = `built "${doc.meta?.name ?? "design"}" \u2014 ${result.created} created, ${result.updated} updated, ${result.deleted} deleted`;
  emitStudio({
    kind: "screen",
    tool: "plumb_design",
    screen: doc.meta?.name ?? null,
    summary
  });
  let authoredPath;
  try {
    authoredPath = join9(tmpdir3(), `plumb-authored-${planId}.json`);
    writeFileSync4(authoredPath, JSON.stringify(compiled.doc));
  } catch {
    authoredPath = void 0;
  }
  return ok({
    source: "plugin",
    planId,
    rootId: result.rootId,
    rootKey: result.rootKey,
    created: result.created,
    updated: result.updated,
    deleted: result.deleted,
    // authored element handle (el / plumbKey) → Figma node id. The join key
    // for plumb_review (elMap) and motion wiring.
    ids: result.ids,
    // Path to the authored PdsDocument — pass to plumb_review as authoredPath.
    authoredPath,
    warnings,
    summary,
    next: "Built. Run plumb_review with { id: rootId, ids, authoredPath } to score built-vs-authored and get the fix list."
  });
}

// src/tools/fig.ts
import { z as z7 } from "zod";
import { existsSync as existsSync6 } from "fs";

// src/fig/parser.ts
import { readFileSync as readFileSync7 } from "fs";
import { parseFig } from "openfig-core";
function loadFig(path) {
  const bytes = readFileSync7(path);
  const doc = parseFig(bytes);
  return buildFigFile(doc);
}
function buildFigFile(doc) {
  const pages = [];
  const screens = [];
  for (const node of doc.nodes) {
    if (node.type !== "CANVAS") continue;
    const pageId = nodeId(node);
    if (!pageId) continue;
    const pageName = node.name || "Untitled page";
    const children = doc.childrenMap.get(pageId) ?? [];
    const frames = [];
    for (const child of children) {
      if (!isScreenLike(child)) continue;
      const childId = nodeId(child);
      if (!childId) continue;
      const entry = {
        id: childId,
        name: child.name || childId,
        w: child.size ? Math.round(child.size.x) : 0,
        h: child.size ? Math.round(child.size.y) : 0,
        page: pageName
      };
      frames.push(entry);
      screens.push(entry);
    }
    pages.push({ id: pageId, name: pageName, frames });
  }
  return { doc, screens, pages };
}
function figNodeById(file, id) {
  return file.doc.nodeMap.get(id) ?? null;
}
var SCREEN_LIKE_TYPES = /* @__PURE__ */ new Set(["FRAME", "COMPONENT", "INSTANCE"]);
function isScreenLike(node) {
  return SCREEN_LIKE_TYPES.has(node.type);
}
function nodeId(node) {
  if (!node.guid) return null;
  return `${node.guid.sessionID}:${node.guid.localID}`;
}

// src/tools/fig.ts
var OUTLINE_DESCRIPTION = "Read a saved .fig file from disk and list every screen (top-level frame) across every page. The headless / CI counterpart to plumb_outline \u2014 works with no Figma desktop, no plugin pairing, no FIGMA_TOKEN. Use this when you've exported a .fig file and need to inspect or implement screens without opening Figma.";
var NODE_DESCRIPTION = "Read one node from a saved .fig file by its id (the `sessionID:localID` form returned by plumb_fig_outline). Returns the node's type, name, size, auto-layout mode, fills, opacity, and (for TEXT nodes) characters and font. Pair with plumb_fig_outline to find the id first.";
function registerPlumbFigOutline(server) {
  server.registerTool(
    "plumb_fig_outline",
    {
      title: "Plumb \xB7 fig outline",
      description: OUTLINE_DESCRIPTION,
      inputSchema: {
        figPath: z7.string().describe("Absolute path to a .fig file on disk.")
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false }
    },
    async ({ figPath }) => {
      try {
        if (!existsSync6(figPath)) {
          throw new PlumbError(
            `No file at ${figPath}.`,
            "Pass the absolute path to a .fig you've exported from Figma. In the Figma desktop app: File \u2192 Save local copy (.fig)."
          );
        }
        const file = loadFig(figPath);
        return ok({
          source: "fig",
          file: { name: file.doc.meta?.fileName ?? figPath },
          meta: { screenCount: file.screens.length },
          pages: file.pages,
          next: file.screens.length === 0 ? "The .fig contains no top-level frames \u2014 confirm the file was exported correctly." : "Call plumb_fig_node with one of these screen ids to fetch a node's details."
        });
      } catch (e) {
        return fail(e);
      }
    }
  );
}
function registerPlumbFigNode(server) {
  server.registerTool(
    "plumb_fig_node",
    {
      title: "Plumb \xB7 fig node",
      description: NODE_DESCRIPTION,
      inputSchema: {
        figPath: z7.string().describe("Absolute path to a .fig file on disk."),
        id: z7.string().describe("Node id \u2014 `sessionID:localID` form.")
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false }
    },
    async ({ figPath, id }) => {
      try {
        if (!existsSync6(figPath)) {
          throw new PlumbError(
            `No file at ${figPath}.`,
            "Pass the absolute path to a .fig file \u2014 same as plumb_fig_outline."
          );
        }
        const file = loadFig(figPath);
        const node = figNodeById(file, id);
        if (!node) {
          throw new PlumbError(
            `Node ${id} not found in ${figPath}.`,
            "Call plumb_fig_outline to list valid screen ids, then drill into one."
          );
        }
        return ok({
          source: "fig",
          node: {
            id,
            name: node.name,
            type: node.type,
            size: node.size ? { w: Math.round(node.size.x), h: Math.round(node.size.y) } : null,
            stackMode: node.stackMode,
            cornerRadius: node.cornerRadius,
            opacity: node.opacity,
            fillCount: Array.isArray(node.fillPaints) ? node.fillPaints.length : 0,
            strokeCount: Array.isArray(node.strokePaints) ? node.strokePaints.length : 0,
            text: node.textData ? {
              characters: node.textData.characters,
              fontSize: node.fontSize,
              fontFamily: node.fontName?.family,
              fontStyle: node.fontName?.style
            } : null
          },
          next: "This MVP exposes basic metadata. Future iterations will return a full PDS with auto-layout \u2192 flexbox, deduped tokens, and stable el handles."
        });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/fit.ts
import { z as z8 } from "zod";

// src/tools/resolveTarget.ts
async function resolveVerifyTarget(args, depth) {
  const { fileKey, id } = resolveFigmaTarget({
    url: args.url,
    fileKey: args.fileKey,
    id: args.id
  });
  const source = bridge.paired && !fileKey ? "plugin" : "rest";
  if (source === "plugin") {
    const resolved = resolveScreen(id, args.name);
    if ("ambiguous" in resolved) {
      return { kind: "ambiguous", matches: formatScreenMatches(resolved.ambiguous) };
    }
    const { doc, nodeName } = await requestNode(resolved.id);
    if (!doc) {
      throw new PlumbError(
        `The Plumb plugin could not find node "${resolved.id}".`,
        "Call plumb_outline for the current screen list \u2014 it may have been deleted or renamed."
      );
    }
    const file2 = {
      document: doc,
      fileName: bridge.inventory?.fileName ?? "",
      version: `plugin-${Date.now()}`
    };
    const pds2 = normalizeToBudget(file2, depth, void 0, { notes: false });
    return { kind: "pds", source, screen: nodeName, pds: pds2 };
  }
  if (!fileKey || !id) {
    throw new PlumbError(
      "Plumb needs the plugin paired (id/name), or a fileKey + id (or a Figma url) for the REST path.",
      "Pair the Plumb plugin in Figma, paste a Figma URL via `url`, or pass both fileKey and id."
    );
  }
  const token = requireToken2();
  const file = await fetchNodeViaRest({ fileKey, nodeId: id, depth: depth + 1, token });
  const pds = normalizeToBudget(file, depth, void 0, { notes: false });
  return { kind: "pds", source, screen: file.document.name, pds };
}

// src/tools/fit.ts
var DESCRIPTION6 = 'Self-healing build loop \u2014 iterate to a pixel-perfect match instead of a one-shot check. The loop:\n  1. Build the component, stamping data-plumb-id="<el>" on each element using the PDS handles (same `el` keys plumb_node/plumb_query return).\n  2. Capture box (getBoundingClientRect) + the getComputedStyle subset (backgroundColor, color, font*, padding*, gap, flex*, justifyContent, alignItems, borderRadius/Color/Width, opacity, textDecorationLine) + text for every tagged element. Same shape as plumb_verify.\n  3. Call plumb_fit with `rendered`. You get back:\n       \u2022 score    \u2014 0\u2013100 convergence (coverage \xD7 fidelity), climbs as you go\n       \u2022 done     \u2014 true once score \u2265 accept (default 98) and no errors remain\n       \u2022 topFixes \u2014 the highest-leverage changes, sorted error-first\n       \u2022 bar      \u2014 a printable \u25B0\u25B1 progress bar\n       \u2022 instruction \u2014 what to do next\n  4. If done=false, apply topFixes, re-render, call plumb_fit again. Repeat until done=true. Each round the score should rise; if it stalls, read the full `deltas` and `coverage.untagged` to see what you missed.';
var renderedElementSchema = z8.object({
  el: z8.string(),
  box: z8.object({ x: z8.number(), y: z8.number(), w: z8.number(), h: z8.number() }),
  text: z8.string().optional(),
  styles: z8.record(z8.string(), z8.string()).optional()
});
function registerPlumbFit(server) {
  server.registerTool(
    "plumb_fit",
    {
      title: "Plumb \xB7 fit",
      description: DESCRIPTION6,
      inputSchema: {
        id: z8.string().optional().describe("Screen id."),
        name: z8.string().optional().describe("Screen name (plugin path)."),
        fileKey: z8.string().optional().describe("File key (REST path)."),
        url: z8.string().optional().describe("Paste a full Figma URL \u2014 fileKey and node-id are auto-extracted."),
        rendered: z8.array(renderedElementSchema).describe('Each element you tagged data-plumb-id="<el>", with box + styles.'),
        iteration: z8.number().int().positive().optional().describe("Which pass this is (1, 2, 3\u2026). Informational \u2014 sharpens the coaching."),
        accept: z8.number().min(0).max(100).optional().describe(`Score at which to stop. Default ${DEFAULT_ACCEPT}.`),
        tolerances: z8.object({
          px: z8.object({ ok: z8.number(), warn: z8.number() }).optional(),
          color: z8.object({ ok: z8.number(), warn: z8.number() }).optional()
        }).optional().describe("Override default thresholds."),
        depth: z8.number().int().positive().optional().describe("PDS depth to fetch. Default 12 \u2014 deep enough for most screens.")
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false }
    },
    async (args) => {
      try {
        if (!args.rendered || args.rendered.length === 0) {
          throw new PlumbError(
            "plumb_fit needs the rendered layout.",
            "For every element you tagged data-plumb-id, send { el, box: getBoundingClientRect(), styles: getComputedStyle subset }. Then call plumb_fit again each round until done=true."
          );
        }
        const depth = args.depth ?? 12;
        const target = await resolveVerifyTarget(args, depth);
        if (target.kind === "ambiguous") {
          return ok({
            ambiguous: true,
            matches: target.matches,
            next: "Several screens share that name. Each row shows `page` and `box` (w\xD7h) \u2014 pick the one you want and re-call plumb_fit with its `id`."
          });
        }
        const { source, screen, pds } = target;
        const tolerances = {
          px: { ...DEFAULT_TOLERANCES.px, ...args.tolerances?.px ?? {} },
          color: { ...DEFAULT_TOLERANCES.color, ...args.tolerances?.color ?? {} }
        };
        const result = verifyAgainst(pds, args.rendered, tolerances);
        const fit = buildFitResponse(result, {
          accept: args.accept,
          iteration: args.iteration
        });
        emitStudio({
          kind: "fit",
          tool: "plumb_fit",
          screen,
          score: fit.score,
          done: fit.done,
          matched: fit.matched,
          importantMatched: fit.importantMatched,
          importantTotal: fit.importantTotal,
          deltas: result.deltas.slice(0, 12).map((d) => ({ el: d.el, kind: d.kind, severity: d.severity })),
          summary: `fit ${fit.score.toFixed(1)}% \u2014 ${fit.errors} err / ${fit.warns} warn${fit.done ? " \xB7 done \u2713" : ""}`
        });
        return ok({
          source,
          screen,
          score: fit.score,
          done: fit.done,
          bar: fit.bar,
          iteration: fit.iteration,
          matched: fit.matched,
          importantMatched: fit.importantMatched,
          importantTotal: fit.importantTotal,
          errors: fit.errors,
          warns: fit.warns,
          topFixes: fit.topFixes,
          instruction: fit.instruction,
          deltas: result.deltas,
          coverage: result.coverage
        });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/node.ts
import { z as z9 } from "zod";
var DESCRIPTION7 = "Extract a Figma screen or node as a compact, normalized Plumb Design Spec (PDS): deduplicated design tokens plus a CSS-shaped node tree, with auto-layout pre-resolved to flexbox. With the Plumb plugin paired, pass a screen `id` or `name` (no file key) \u2014 duplicate names come back as a match list to disambiguate. On the REST path, pass `fileKey` + `id`.";
function registerPlumbNode(server) {
  server.registerTool(
    "plumb_node",
    {
      title: "Plumb \xB7 node",
      description: DESCRIPTION7,
      inputSchema: {
        fileKey: z9.string().optional().describe("Figma file key \u2014 REST path. Omit when the Plumb plugin is paired."),
        url: z9.string().optional().describe(
          "Paste a full Figma URL \u2014 fileKey and node-id are auto-extracted (the `-` separator is normalised to `:`)."
        ),
        id: z9.string().optional().describe("Node/screen id to extract."),
        name: z9.string().optional().describe("Screen name \u2014 plugin path; resolved against the paired file."),
        depth: z9.number().int().min(1).max(12).optional().describe("Levels to disclose. Default 3."),
        expandAll: z9.boolean().optional().describe(
          "Walk the entire subtree in one call, ignoring `depth`. Subject to `maxTokens` (defaults to 60000 if omitted); if the spec exceeds the budget, depth is auto-trimmed and `meta.truncated` is set. Use this to skip the drill-loop on dense screens."
        ),
        notes: z9.boolean().optional().describe("Include human-readable notes per node."),
        maxTokens: z9.number().int().positive().optional().describe("Soft token budget; fit-to-budget reduces depth to fit.")
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: true }
    },
    async (args) => {
      try {
        const expandAll = args.expandAll === true;
        const depth = expandAll ? 12 : args.depth ?? 3;
        const maxTokens = args.maxTokens ?? (expandAll ? 6e4 : void 0);
        const { fileKey, id } = resolveFigmaTarget({
          url: args.url,
          fileKey: args.fileKey,
          id: args.id
        });
        if (bridge.paired && (id || args.name)) {
          const resolved = resolveScreen(id, args.name);
          if ("ambiguous" in resolved) {
            return ok({
              ambiguous: true,
              matches: formatScreenMatches(resolved.ambiguous),
              next: "Several screens share that name. Each row shows `page` and `box` (w\xD7h) \u2014 pick the one you want and re-call plumb_node with its `id`."
            });
          }
          const pluginDepth = expandAll ? void 0 : depth + 1;
          const { doc, nodeName } = await requestNode(resolved.id, pluginDepth);
          if (!doc) {
            throw new PlumbError(
              `The Plumb plugin could not find node "${resolved.id}".`,
              "Call plumb_outline for the current screen list \u2014 it may have been deleted or renamed."
            );
          }
          const file2 = {
            document: doc,
            fileName: bridge.inventory?.fileName ?? "",
            version: `plugin-${Date.now()}`
          };
          const pds2 = normalizeToBudget(file2, depth, maxTokens, { notes: args.notes });
          emitStudio({ kind: "screen", tool: "plumb_node", screen: nodeName, summary: `extracted ${nodeName}` });
          return ok({ ...pds2, source: "plugin", node: nodeName });
        }
        if (!fileKey || !id) {
          throw new PlumbError(
            "plumb_node needs the Plumb plugin paired, or a fileKey + id (or a Figma url) for the REST path.",
            "Pair the Plumb plugin in Figma, paste a Figma URL via `url`, or pass both fileKey and id."
          );
        }
        const cacheKey = `node:${fileKey}:${id}:${depth}:${args.notes ? 1 : 0}:${maxTokens ?? 0}:${expandAll ? 1 : 0}`;
        const hit = cacheGet(cacheKey, DEFAULT_TTL_MS);
        if (hit) return ok({ ...hit.payload, source: "rest", cached: true });
        const token = requireToken2();
        const file = await fetchNodeViaRest({
          fileKey,
          nodeId: id,
          depth: expandAll ? 12 : depth + 1,
          token
        });
        const pds = normalizeToBudget(file, depth, maxTokens, { notes: args.notes });
        cacheSet(cacheKey, file.version, pds);
        emitStudio({ kind: "screen", tool: "plumb_node", screen: file.document.name, summary: `extracted ${file.document.name}` });
        return ok({ ...pds, source: "rest", cached: false });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/outline.ts
import { z as z10 } from "zod";

// src/normalize/outline.ts
function buildOutline(document, file) {
  const minter = new HandleMinter();
  const pages = [];
  for (const page of document.children ?? []) {
    if (page.visible === false) continue;
    const frames = [];
    for (const frame of page.children ?? []) {
      if (frame.visible === false) continue;
      frames.push({
        id: frame.id,
        el: minter.mint(frame.name ?? "", frame.type ?? ""),
        name: frame.name ?? "",
        type: (frame.type ?? "").toLowerCase(),
        box: {
          w: round(frame.absoluteBoundingBox?.width ?? 0),
          h: round(frame.absoluteBoundingBox?.height ?? 0)
        }
      });
    }
    pages.push({ id: page.id, name: page.name ?? "", frames });
  }
  const frameCount = pages.reduce((n, p) => n + p.frames.length, 0);
  const outline = {
    file,
    pages,
    meta: { pageCount: pages.length, frameCount, estTokens: 0 },
    next: "Pick a frame, then call plumb_node with this fileKey and the frame's `id` at depth 2-3. Drill into any node that has a `more` count."
  };
  outline.meta.estTokens = estimateTokens(JSON.stringify(outline));
  return outline;
}

// src/tools/outline.ts
function filterByPage(outline, pageFilter) {
  if (!pageFilter) return outline;
  const pages = outline.pages ?? [];
  const kept = pages.filter((p) => (p?.name ?? "").toLowerCase() === pageFilter);
  if (kept.length === 0) {
    const fuzzy = pages.filter((p) => (p?.name ?? "").toLowerCase().includes(pageFilter));
    if (fuzzy.length) return rebuildOutline(outline, fuzzy);
  }
  return rebuildOutline(outline, kept);
}
function rebuildOutline(outline, pages) {
  const screenCount = pages.reduce(
    (n, p) => n + (p.frames ?? p.screens ?? []).length,
    0
  );
  const meta = { ...outline.meta ?? {} };
  if ("frameCount" in meta) meta.frameCount = screenCount;
  if ("screenCount" in meta) meta.screenCount = screenCount;
  meta.pageCount = pages.length;
  return { ...outline, pages, meta };
}
var DESCRIPTION8 = "Map a Figma file cheaply: its pages and their top-level screens (id, name, size). The shallow entry point \u2014 call it to find the screen you want, then call plumb_node with that screen's id (or name) to extract it. With the Plumb plugin paired, no file key is needed.";
function registerPlumbOutline(server) {
  server.registerTool(
    "plumb_outline",
    {
      title: "Plumb \xB7 outline",
      description: DESCRIPTION8,
      inputSchema: {
        fileKey: z10.string().optional().describe("Figma file key \u2014 for the REST path. Omit when the Plumb plugin is paired."),
        url: z10.string().optional().describe(
          "Paste a full Figma URL \u2014 fileKey is auto-extracted. Accepts /design/, /file/, /proto/, and branch URLs."
        ),
        page: z10.string().optional().describe(
          "Filter to a single Figma page by name (case-insensitive). Saves tokens on multi-page files where most pages are irrelevant."
        )
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: true }
    },
    async (args) => {
      try {
        const { fileKey } = resolveFigmaTarget({ url: args.url, fileKey: args.fileKey });
        const pageFilter = args.page?.trim().toLowerCase();
        if (bridge.paired && bridge.inventory && !fileKey) {
          return ok(
            filterByPage(
              pluginOutline(),
              pageFilter
            )
          );
        }
        if (!fileKey) {
          throw new PlumbError(
            "plumb_outline needs the Plumb plugin paired, or a fileKey / url for the REST path.",
            "Pair the Plumb plugin in Figma, paste a Figma URL via `url`, or pass a fileKey."
          );
        }
        const cacheKey = `outline:${fileKey}`;
        const hit = cacheGet(cacheKey, DEFAULT_TTL_MS);
        if (hit) return ok({ ...filterByPage(hit.payload, pageFilter), cached: true });
        const token = requireToken2();
        const file = await fetchFileOutline(fileKey, token);
        const outline = buildOutline(file.document, {
          key: fileKey,
          name: file.fileName,
          version: file.version
        });
        cacheSet(cacheKey, file.version, outline);
        return ok({ ...filterByPage(outline, pageFilter), cached: false });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/query.ts
import { z as z11 } from "zod";
var DESCRIPTION9 = "Query a Figma subtree by pattern instead of dumping the whole tree. Use this on dense screens where plumb_node would be too big \u2014 pull a skeleton (structure only, no text/fills/effects), every button, every TEXT node above a size, every instance of a component. Mirrors the same scope-resolution as plumb_node: pass `id` or `name` with the plugin paired, or `fileKey` + `id` (or a Figma URL) on the REST path.";
function registerPlumbQuery(server) {
  server.registerTool(
    "plumb_query",
    {
      title: "Plumb \xB7 query",
      description: DESCRIPTION9,
      inputSchema: {
        fileKey: z11.string().optional().describe("Figma file key \u2014 REST path. Omit when the plugin is paired."),
        url: z11.string().optional().describe("Paste a full Figma URL \u2014 fileKey and node-id auto-extracted."),
        id: z11.string().optional().describe("Node/screen id to query within."),
        name: z11.string().optional().describe("Screen name \u2014 plugin path, resolved against the paired file."),
        select: z11.enum(["skeleton", "buttons", "text", "components"]).describe(
          'Query pattern. "skeleton" = structure-only (drops chars, fills, effects, vectorPath, text refs). "buttons" = nodes Plumb tagged with pattern: button. "text" = TEXT nodes, optionally filtered by font-size min/max. "components" = INSTANCE nodes, optionally filtered to a specific componentId.'
        ),
        min: z11.number().optional().describe('Minimum font size for select: "text".'),
        max: z11.number().optional().describe('Maximum font size for select: "text".'),
        componentId: z11.string().optional().describe('Filter for select: "components" \u2014 return only instances of this component.')
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: true }
    },
    async (args) => {
      try {
        const { fileKey, id } = resolveFigmaTarget({
          url: args.url,
          fileKey: args.fileKey,
          id: args.id
        });
        let pds;
        let nodeName = null;
        if (bridge.paired && (id || args.name)) {
          const resolved = resolveScreen(id, args.name);
          if ("ambiguous" in resolved) {
            return ok({
              ambiguous: true,
              matches: formatScreenMatches(resolved.ambiguous),
              next: "Several screens share that name. Pick one and re-call plumb_query with its `id`."
            });
          }
          const { doc, nodeName: n } = await requestNode(resolved.id);
          if (!doc) {
            throw new PlumbError(
              `The Plumb plugin could not find node "${resolved.id}".`,
              "Call plumb_outline for the current screen list."
            );
          }
          nodeName = n;
          const file = {
            document: doc,
            fileName: bridge.inventory?.fileName ?? "",
            version: `plugin-${Date.now()}`
          };
          pds = normalize2(file, 12, {});
        } else {
          if (!fileKey || !id) {
            throw new PlumbError(
              "plumb_query needs the Plumb plugin paired, or fileKey + id (or a Figma url) for REST.",
              "Pair the plugin in Figma, pass a Figma URL via `url`, or pass fileKey and id."
            );
          }
          const token = requireToken2();
          const file = await fetchNodeViaRest({ fileKey, nodeId: id, depth: 12, token });
          pds = normalize2(file, 12, {});
        }
        const matches = applySelect(pds, args.select, {
          min: args.min,
          max: args.max,
          componentId: args.componentId
        });
        const result = {
          scope: { id: id ?? args.name ?? "", name: nodeName },
          select: args.select,
          matches,
          meta: {
            count: matches.length,
            estTokens: estimateTokens(JSON.stringify(matches))
          },
          next: args.select === "skeleton" ? "Use the skeleton to grasp shape; call plumb_node for full data on the branches you need." : "Drill into a match with plumb_node({ id: match.id }) for full detail."
        };
        return ok({ ...result, tokens: pds.tokens, source: bridge.paired ? "plugin" : "rest" });
      } catch (e) {
        return fail(e);
      }
    }
  );
}
function applySelect(pds, select, args) {
  const all = Object.values(pds.nodes);
  switch (select) {
    case "skeleton":
      return all.map((n) => {
        const skeleton = {
          id: n.id,
          el: n.el,
          type: n.type,
          box: n.box
        };
        if (n.name) skeleton.name = n.name;
        if (n.layout) skeleton.layout = n.layout;
        if (n.pos) skeleton.pos = n.pos;
        if (n.children) skeleton.children = n.children;
        if (n.more) skeleton.more = n.more;
        if (n.pattern) skeleton.pattern = n.pattern;
        if (n.component) skeleton.component = n.component;
        return skeleton;
      });
    case "buttons":
      return all.filter((n) => n.pattern === "button");
    case "text": {
      const textNodes = all.filter((n) => n.type === "text");
      if (args.min === void 0 && args.max === void 0) return textNodes;
      const sizeOf = (n) => {
        const ref = n.text;
        if (!ref) return void 0;
        const tok = pds.tokens.text[ref];
        if (!tok) return void 0;
        const m = /\s(\d+(?:\.\d+)?)px/.exec(tok);
        return m ? Number(m[1]) : void 0;
      };
      return textNodes.filter((n) => {
        const size = sizeOf(n);
        if (size === void 0) return false;
        if (args.min !== void 0 && size < args.min) return false;
        if (args.max !== void 0 && size > args.max) return false;
        return true;
      });
    }
    case "components": {
      const instances = all.filter((n) => n.type === "instance");
      if (!args.componentId) return instances;
      return instances.filter((n) => {
        if (typeof n.component === "string") return n.component === args.componentId;
        if (n.component && typeof n.component === "object") return n.component.id === args.componentId;
        return false;
      });
    }
  }
}

// src/tools/review.ts
import { existsSync as existsSync7, readFileSync as readFileSync8 } from "fs";
import { z as z12 } from "zod";

// src/review/contrast.ts
var AA_NORMAL = 4.5;
var AA_LARGE = 3;
function toRgb(color) {
  const c = parseColor(color);
  if (!c) return null;
  return { r: c.r, g: c.g, b: c.b };
}
function linearize(channel2) {
  const x = channel2 / 255;
  return x <= 0.03928 ? x / 12.92 : Math.pow((x + 0.055) / 1.055, 2.4);
}
function relativeLuminance(rgb2) {
  return 0.2126 * linearize(rgb2.r) + 0.7152 * linearize(rgb2.g) + 0.0722 * linearize(rgb2.b);
}
function contrastRatio(a, b) {
  const la = relativeLuminance(a);
  const lb = relativeLuminance(b);
  const lighter = Math.max(la, lb);
  const darker = Math.min(la, lb);
  return (lighter + 0.05) / (darker + 0.05);
}
function aaThreshold(sizePx, weight) {
  const size = sizePx ?? 16;
  const bold = (weight ?? 400) >= 700;
  if (size >= 24 || bold && size >= 18.66) return AA_LARGE;
  return AA_NORMAL;
}

// src/review/pdsAdapter.ts
function resolveColor2(ref, tokens) {
  if (!ref) return void 0;
  if (ref.startsWith("$c")) return tokens.color[ref];
  return ref;
}
function cssAlignSelf(selfAlign) {
  switch (selfAlign) {
    case "stretch":
      return "stretch";
    case "min":
      return "flex-start";
    case "max":
      return "flex-end";
    case "center":
      return "center";
    default:
      return void 0;
  }
}
function stylesForNode(node, tokens) {
  const styles = {};
  const isText = node.type === "text";
  const fillHex = resolveColor2(node.fill, tokens);
  if (fillHex) {
    if (isText) styles.color = fillHex;
    else styles.backgroundColor = fillHex;
  }
  const layout = resolveLayout(node.layout, tokens);
  if (layout) {
    styles.flexDirection = layout.flow === "col" ? "column" : "row";
    if (layout.gap !== void 0) styles.gap = `${layout.gap}px`;
    const [t, r, b, l] = layout.pad;
    styles.paddingTop = `${t}px`;
    styles.paddingRight = `${r}px`;
    styles.paddingBottom = `${b}px`;
    styles.paddingLeft = `${l}px`;
    if (layout.justify) styles.justifyContent = layout.justify;
    if (layout.align) styles.alignItems = layout.align;
  }
  if (typeof node.text === "string" && node.text.startsWith("$t")) {
    const raw = tokens.text[node.text];
    const parsed = raw ? parseTextToken(raw) : null;
    if (parsed) {
      styles.fontSize = `${parsed.size}px`;
      styles.fontWeight = String(parsed.weight);
      if (parsed.lh !== void 0) styles.lineHeight = `${parsed.lh * parsed.size}px`;
      if (parsed.family) styles.fontFamily = parsed.family;
    }
  }
  if (isText && node.textDecoration) styles.textDecorationLine = node.textDecoration;
  if (node.radius !== void 0) {
    let px = null;
    if (typeof node.radius === "string") {
      const resolved = tokens.radius[node.radius];
      px = resolved === "full" ? 9999 : resolved ?? null;
    } else if (Array.isArray(node.radius)) {
      px = node.radius[0] ?? null;
    }
    if (px !== null) styles.borderRadius = `${px}px`;
  }
  const strokeHex = resolveColor2(node.stroke, tokens);
  if (strokeHex) styles.borderColor = strokeHex;
  if (node.strokeW !== void 0) styles.borderWidth = `${node.strokeW}px`;
  if (typeof node.opacity === "number") styles.opacity = String(node.opacity);
  const shadow = typeof node.shadow === "string" && node.shadow.startsWith("$s") ? tokens.shadow[node.shadow] : typeof node.shadow === "string" ? node.shadow : void 0;
  if (shadow) styles.boxShadow = shadow;
  if (node.backdropFilter) styles.backdropFilter = node.backdropFilter;
  if (typeof node.grow === "number" && node.grow > 0) styles.flexGrow = String(node.grow);
  const alignSelf = cssAlignSelf(node.selfAlign);
  if (alignSelf) styles.alignSelf = alignSelf;
  const fills = typeof node.fills === "string" ? tokens.fills?.[node.fills] : node.fills;
  if (Array.isArray(fills) && fills.length > 1) {
    styles.backgroundImage = fills.map(() => "linear-gradient(#000, #000)").join(", ");
  }
  return styles;
}
function pdsToRendered(built) {
  const out = [];
  for (const el of Object.keys(built.nodes)) {
    const node = built.nodes[el];
    if (!node) continue;
    const el2 = {
      el,
      box: {
        x: node.pos?.x ?? 0,
        y: node.pos?.y ?? 0,
        w: node.box.w,
        h: node.box.h
      },
      styles: stylesForNode(node, built.tokens)
    };
    if (typeof node.chars === "string") el2.text = node.chars;
    const hasAsset = typeof node.assetId === "string" && node.assetId.length > 0;
    const isVectorish = node.type === "vector" || node.type === "image" || node.vectorPath !== void 0;
    if (hasAsset) {
      el2.asset = node.assetId;
      el2.img = true;
    } else if (isVectorish) {
      el2.img = true;
    }
    out.push(el2);
  }
  return out;
}
function remapBuiltToAuthored(built, ids) {
  const remap = /* @__PURE__ */ new Map();
  if (!ids) return remap;
  const figIdToAuthored = /* @__PURE__ */ new Map();
  for (const authoredEl of Object.keys(ids)) {
    const figId = ids[authoredEl];
    if (figId) figIdToAuthored.set(figId, authoredEl);
  }
  for (const el of Object.keys(built.nodes)) {
    const node = built.nodes[el];
    if (!node) continue;
    const authoredEl = figIdToAuthored.get(node.id);
    if (authoredEl) remap.set(el, authoredEl);
  }
  return remap;
}
function applyRemap(rendered, remap) {
  if (remap.size === 0) return rendered;
  return rendered.map((r) => {
    const authored = remap.get(r.el);
    return authored ? { ...r, el: authored } : r;
  });
}

// src/review/rubric.ts
var WEIGHTS = {
  hierarchy: 0.2,
  spacing: 0.15,
  contrast: 0.25,
  alignment: 0.15,
  "type-scale": 0.15,
  polish: 0.1
};
var LABELS = {
  hierarchy: "Visual hierarchy",
  spacing: "Spacing rhythm",
  contrast: "Contrast & a11y",
  alignment: "Alignment",
  "type-scale": "Type scale",
  polish: "Professional polish"
};
function clamp2(n, lo = 0, hi = 100) {
  return Math.max(lo, Math.min(hi, n));
}
function typeOf(node, tokens) {
  if (node.type !== "text") return null;
  if (typeof node.text !== "string" || !node.text.startsWith("$t")) return null;
  const raw = tokens.text[node.text];
  const parsed = raw ? parseTextToken(raw) : null;
  if (!parsed) return null;
  return { el: node.el, size: parsed.size, weight: parsed.weight, family: parsed.family };
}
function buildCtx(pds, _brief) {
  const tokens = pds.tokens;
  const reachable = [];
  const parent = /* @__PURE__ */ new Map();
  const seen = /* @__PURE__ */ new Set();
  const queue = [pds.root];
  while (queue.length) {
    const el = queue.shift();
    if (!el || seen.has(el)) continue;
    seen.add(el);
    reachable.push(el);
    const node = pds.nodes[el];
    if (node?.children) {
      for (const c of node.children) {
        if (!parent.has(c)) parent.set(c, el);
        queue.push(c);
      }
    }
  }
  const ctx = {
    pds,
    tokens,
    reachable,
    parent,
    types: [],
    spacings: [],
    placeable: 0,
    absolute: 0,
    hasShadow: false,
    hasRadius: false,
    fillHexes: /* @__PURE__ */ new Set(),
    placeholderTexts: 0,
    totalTexts: 0
  };
  for (const el of reachable) {
    const node = pds.nodes[el];
    if (!node) continue;
    const ti = typeOf(node, tokens);
    if (ti) ctx.types.push(ti);
    const layout = resolveLayout(node.layout, tokens);
    if (layout) {
      if (layout.gap) ctx.spacings.push(layout.gap);
      for (const p of layout.pad) if (p > 0) ctx.spacings.push(p);
    }
    if (el !== pds.root) ctx.placeable += 1;
    if (node.pos) ctx.absolute += 1;
    if (node.shadow || node.effects) ctx.hasShadow = true;
    if (node.radius !== void 0) {
      const r = typeof node.radius === "string" ? tokens.radius[node.radius] : node.radius[0];
      if (r === "full" || typeof r === "number" && r > 0) ctx.hasRadius = true;
    }
    if (typeof node.fill === "string" && node.fill.startsWith("$c")) {
      const hex = tokens.color[node.fill];
      if (hex) ctx.fillHexes.add(hex.toLowerCase());
    }
    if (node.type === "text" && typeof node.chars === "string") {
      ctx.totalTexts += 1;
      if (isPlaceholderText(node.chars.trim(), false)) ctx.placeholderTexts += 1;
    }
  }
  return ctx;
}
function backgroundHex(el, ctx) {
  let cur = el;
  const guard = /* @__PURE__ */ new Set();
  while (cur && !guard.has(cur)) {
    guard.add(cur);
    const node = ctx.pds.nodes[cur];
    if (node) {
      const ref = typeof node.fill === "string" && node.fill.startsWith("$c") ? node.fill : node.inheritedFill;
      if (cur !== el && ref) {
        const hex = ctx.tokens.color[ref];
        if (hex) return hex;
      }
    }
    cur = ctx.parent.get(cur);
  }
  return void 0;
}
function scoreTypeScale(ctx, brief) {
  const issues = [];
  const sizes = ctx.types.map((t) => t.size);
  const distinct = Array.from(new Set(sizes)).sort((a, b) => a - b);
  let score = 100;
  if (sizes.length === 0) {
    return { id: "type-scale", label: LABELS["type-scale"], score: 70, weight: WEIGHTS["type-scale"], issues };
  }
  if (distinct.length > 6) {
    const over = distinct.length - 6;
    score -= Math.min(40, over * 8);
    issues.push({
      dimension: "type-scale",
      severity: "warn",
      message: `${distinct.length} distinct font sizes \u2014 that's not a scale, it's a pile.`,
      fix: "Collapse to a 4\u20136 step modular scale (e.g. 12/14/16/20/28/40) and reuse the steps."
    });
  } else if (distinct.length === 1 && ctx.types.length > 2) {
    score -= 15;
    issues.push({
      dimension: "type-scale",
      severity: "warn",
      message: "Every text node is the same size \u2014 no typographic scale at all.",
      fix: "Introduce at least a heading vs body size step so the scale can carry hierarchy."
    });
  }
  if (distinct.length >= 3) {
    const ratios = [];
    for (let i = 1; i < distinct.length; i++) {
      const prev = distinct[i - 1];
      const cur = distinct[i];
      if (prev > 0) ratios.push(cur / prev);
    }
    if (ratios.length) {
      const mean = ratios.reduce((a, b) => a + b, 0) / ratios.length;
      const variance = ratios.reduce((a, b) => a + (b - mean) ** 2, 0) / ratios.length;
      if (variance > 0.08) {
        score -= 15;
        issues.push({
          dimension: "type-scale",
          severity: "info",
          message: "Font-size steps are irregular \u2014 the ratios between sizes jump around.",
          fix: "Pick a consistent ratio (~1.2\u20131.33) between adjacent scale steps."
        });
      }
    }
  }
  if (brief?.typeSizes && brief.typeSizes.length) {
    const expected = brief.typeSizes;
    const off = distinct.filter(
      (s) => !expected.some((e) => Math.abs(e - s) <= 1)
    );
    if (off.length) {
      score -= Math.min(20, off.length * 6);
      issues.push({
        dimension: "type-scale",
        severity: "warn",
        message: `Font sizes ${off.join(", ")}px aren't in the brief's type scale.`,
        fix: `Snap them to the intended scale: ${expected.join(", ")}px.`
      });
    }
  }
  return { id: "type-scale", label: LABELS["type-scale"], score: clamp2(score), weight: WEIGHTS["type-scale"], issues };
}
function scoreHierarchy(ctx) {
  const issues = [];
  const sizes = ctx.types.map((t) => t.size).sort((a, b) => b - a);
  if (sizes.length < 2) {
    return { id: "hierarchy", label: LABELS.hierarchy, score: 70, weight: WEIGHTS.hierarchy, issues };
  }
  const top = sizes[0];
  const counts = /* @__PURE__ */ new Map();
  for (const s of ctx.types.map((t) => t.size)) counts.set(s, (counts.get(s) ?? 0) + 1);
  let body = sizes[sizes.length - 1];
  let best = -1;
  for (const [s, c] of counts) {
    if (c > best || c === best && s < body) {
      best = c;
      body = s;
    }
  }
  const ratio = body > 0 ? top / body : 1;
  let score = 100;
  if (ratio < 1.2) {
    score = 45;
    issues.push({
      dimension: "hierarchy",
      severity: "warn",
      message: `Largest text is only ${ratio.toFixed(2)}\xD7 the body size \u2014 hierarchy reads flat.`,
      fix: "Make the primary heading clearly dominant (aim for \u22651.5\xD7 the body size), or add weight/colour contrast."
    });
  } else if (ratio < 1.5) {
    score = 75;
    issues.push({
      dimension: "hierarchy",
      severity: "info",
      message: `Heading-to-body ratio is modest (${ratio.toFixed(2)}\xD7).`,
      fix: "Consider a bolder size or weight step to strengthen the focal point."
    });
  }
  const weights = new Set(ctx.types.map((t) => t.weight));
  if (weights.size === 1 && ctx.types.length > 3) {
    score -= 10;
    issues.push({
      dimension: "hierarchy",
      severity: "info",
      message: "Every text node shares one font weight.",
      fix: "Use a heavier weight on headings/labels to separate levels."
    });
  }
  return { id: "hierarchy", label: LABELS.hierarchy, score: clamp2(score), weight: WEIGHTS.hierarchy, issues };
}
function scoreSpacing(ctx, brief) {
  const issues = [];
  const vals = ctx.spacings;
  if (vals.length === 0) {
    return { id: "spacing", label: LABELS.spacing, score: 80, weight: WEIGHTS.spacing, issues };
  }
  const base = brief?.spacingBase ?? (vals.filter((v) => v % 8 === 0).length / vals.length >= 0.6 ? 8 : 4);
  const onGrid = vals.filter((v) => v % base === 0).length;
  const onGridShare = onGrid / vals.length;
  let score = Math.round(onGridShare * 100);
  if (onGridShare < 0.85) {
    const off = Array.from(new Set(vals.filter((v) => v % base !== 0))).sort((a, b) => a - b);
    issues.push({
      dimension: "spacing",
      severity: onGridShare < 0.6 ? "warn" : "info",
      message: `${Math.round((1 - onGridShare) * 100)}% of gaps/padding are off the ${base}px grid (${off.slice(0, 8).join(", ")}${off.length > 8 ? "\u2026" : ""}).`,
      fix: `Snap spacing to multiples of ${base}px so the layout has a consistent rhythm.`
    });
  }
  const distinct = new Set(vals).size;
  if (distinct > 8) {
    score -= Math.min(25, (distinct - 8) * 4);
    issues.push({
      dimension: "spacing",
      severity: "info",
      message: `${distinct} distinct spacing values in use.`,
      fix: "Restrict to a small spacing scale (e.g. 4/8/12/16/24/32/48)."
    });
  }
  return { id: "spacing", label: LABELS.spacing, score: clamp2(score), weight: WEIGHTS.spacing, issues };
}
function scoreContrast(ctx) {
  const issues = [];
  let checked = 0;
  let passed = 0;
  for (const t of ctx.types) {
    const node = ctx.pds.nodes[t.el];
    if (!node) continue;
    if (typeof node.fill !== "string" || !node.fill.startsWith("$c")) continue;
    const fgHex = ctx.tokens.color[node.fill];
    const bgHex = backgroundHex(t.el, ctx);
    if (!bgHex) continue;
    const fg = toRgb(fgHex);
    const bg = toRgb(bgHex);
    if (!fg || !bg) continue;
    checked += 1;
    const ratio = contrastRatio(fg, bg);
    const need = aaThreshold(t.size, t.weight);
    if (ratio >= need) {
      passed += 1;
    } else {
      issues.push({
        dimension: "contrast",
        severity: "error",
        el: t.el,
        message: `Text contrast ${ratio.toFixed(2)}:1 fails WCAG AA (needs ${need}:1) \u2014 ${fgHex} on ${bgHex}.`,
        fix: `Darken the text or lighten the background until it clears ${need}:1.`
      });
    }
  }
  if (checked === 0) {
    return { id: "contrast", label: LABELS.contrast, score: 85, weight: WEIGHTS.contrast, issues };
  }
  const score = Math.round(passed / checked * 100);
  return { id: "contrast", label: LABELS.contrast, score: clamp2(score), weight: WEIGHTS.contrast, issues };
}
function scoreAlignment(ctx) {
  const issues = [];
  if (ctx.placeable === 0) {
    return { id: "alignment", label: LABELS.alignment, score: 80, weight: WEIGHTS.alignment, issues };
  }
  const absShare = ctx.absolute / ctx.placeable;
  let score = Math.round((1 - absShare) * 100);
  if (absShare > 0.25) {
    issues.push({
      dimension: "alignment",
      severity: absShare > 0.5 ? "warn" : "info",
      message: `${Math.round(absShare * 100)}% of nodes are absolutely positioned.`,
      fix: "Wrap ad-hoc absolute children in auto-layout frames so alignment is structural, not hand-placed."
    });
  }
  return { id: "alignment", label: LABELS.alignment, score: clamp2(score), weight: WEIGHTS.alignment, issues };
}
function scorePolish(ctx) {
  const issues = [];
  let score = 40;
  if (ctx.hasShadow) score += 15;
  else
    issues.push({
      dimension: "polish",
      severity: "info",
      message: "No elevation anywhere \u2014 surfaces read flat/templated.",
      fix: "Add a subtle shadow to cards/overlays to create depth where it helps."
    });
  if (ctx.hasRadius) score += 10;
  else
    issues.push({
      dimension: "polish",
      severity: "info",
      message: "No rounded corners \u2014 everything is hard-edged.",
      fix: "Apply a consistent corner radius to buttons/cards/inputs unless a sharp look is intended."
    });
  const palette = ctx.fillHexes.size;
  if (palette >= 3 && palette <= 12) score += 15;
  else if (palette > 12)
    issues.push({
      dimension: "polish",
      severity: "info",
      message: `${palette} distinct fill colours \u2014 the palette looks unmanaged.`,
      fix: "Consolidate to a small brand palette (bg/surface/text/primary + a few accents)."
    });
  else if (palette <= 2)
    issues.push({
      dimension: "polish",
      severity: "info",
      message: "Palette is essentially black on white.",
      fix: "Introduce brand colour and considered greys so it doesn't read as an unstyled wireframe."
    });
  if (ctx.totalTexts > 0) {
    const fillerShare = ctx.placeholderTexts / ctx.totalTexts;
    if (fillerShare < 0.5) score += 20;
    else
      issues.push({
        dimension: "polish",
        severity: "warn",
        message: `${Math.round(fillerShare * 100)}% of text is placeholder/filler.`,
        fix: "Replace lorem/stub copy with real, specific content \u2014 filler is the #1 templated tell."
      });
  } else {
    score += 10;
  }
  return { id: "polish", label: LABELS.polish, score: clamp2(score), weight: WEIGHTS.polish, issues };
}
var RANK = { error: 0, warn: 1, info: 2 };
function critiqueDesign(pds, brief) {
  const ctx = buildCtx(pds, brief);
  const dimensions = [
    scoreHierarchy(ctx),
    scoreSpacing(ctx, brief),
    scoreContrast(ctx),
    scoreAlignment(ctx),
    scoreTypeScale(ctx, brief),
    scorePolish(ctx)
  ];
  const overall = Math.round(
    dimensions.reduce((sum, d) => sum + d.score * d.weight, 0)
  );
  const issues = dimensions.flatMap((d) => d.issues).sort((a, b) => RANK[a.severity] - RANK[b.severity]);
  return { overall: clamp2(overall), dimensions, issues };
}

// src/review/director.ts
var RUBRIC_DIMENSIONS = [
  "hierarchy",
  "spacing",
  "contrast",
  "alignment",
  "type-scale",
  "polish"
];
var SEVERITIES = ["error", "warn", "info"];
function clamp3(n, lo = 0, hi = 100) {
  return Math.max(lo, Math.min(hi, n));
}
function coerceIssue(raw) {
  if (!raw || typeof raw !== "object") return null;
  const r = raw;
  const dimension = RUBRIC_DIMENSIONS.includes(r.dimension) ? r.dimension : "polish";
  const severity = SEVERITIES.includes(r.severity) ? r.severity : "info";
  const message = typeof r.message === "string" ? r.message : "";
  const fix = typeof r.fix === "string" ? r.fix : "";
  if (!message && !fix) return null;
  const el = typeof r.el === "string" ? r.el : void 0;
  return { dimension, severity, message, fix, ...el ? { el } : {} };
}
function coerceDirectorVerdict(raw) {
  if (!raw || typeof raw !== "object") {
    return { score: 0, issues: [] };
  }
  const r = raw;
  const score = typeof r.score === "number" && Number.isFinite(r.score) ? clamp3(r.score) : 0;
  const verdict = typeof r.verdict === "string" ? r.verdict : void 0;
  const rawIssues = Array.isArray(r.issues) ? r.issues : [];
  const issues = rawIssues.map(coerceIssue).filter((i) => i !== null);
  return { score, issues, ...verdict ? { verdict } : {} };
}
function directorGuidance() {
  return 'Act as a demanding creative director reviewing a rendered screenshot. Grade the render you SEE \u2014 not the intent, not the spec, the actual pixels. Be a demanding director, not a lenient one: most first passes deserve real critique.\n\nGrade against these six dimensions (the same a deterministic design rubric uses, so your verdict is directly comparable):\n  hierarchy    \u2014 is there a clear dominant heading vs body, does the eye know where to land first?\n  spacing      \u2014 does the whitespace sit on a consistent, optically-even rhythm?\n  contrast     \u2014 is text legible against its background at a glance?\n  alignment    \u2014 does everything line up, or does it look ad-hoc/hand-placed?\n  type-scale   \u2014 a small, harmonious set of font sizes, or a pile of arbitrary ones?\n  polish       \u2014 crafted and considered, or does it read as a default template?\n\nPlus the things only a rendered image reveals \u2014 fold these into whichever dimension fits (usually polish or hierarchy):\n  - visual balance / weight distribution across the canvas\n  - focal flow \u2014 does the eye land where it should, then travel correctly through the screen\n  - image composition, crop, subject placement, and whether photos/logos actually read\n  - optical spacing \u2014 not grid math, does it *look* even\n  - "designed vs generated" gestalt \u2014 does this look like a human designed it, or like a template a machine emitted\n\nReserve "error" severity for a genuinely broken screen: unreadable text, a broken or redrawn logo, overlapping content, or something that would embarrass the team if shipped.\n\nThen call plumb_review again, passing your grade as the `director` argument:\n{ "score": <0-100>, "verdict": "<one paragraph, your honest gut read>", "issues": [ { "dimension": "hierarchy|spacing|contrast|alignment|type-scale|polish", "severity": "error|warn|info", "message": "<what\'s wrong>", "fix": "<one concrete change>" } ] }';
}

// src/review/index.ts
var STRUCTURE_WEIGHT = 0.5;
var DESIGN_WEIGHT = 0.5;
var DEFAULT_REVIEW_ACCEPT = 90;
var RANK2 = { error: 0, warn: 1, info: 2 };
function fmtVal3(v) {
  if (v === null) return "\u2014";
  return typeof v === "string" ? `"${v}"` : String(v);
}
function fmtDelta(d) {
  const where = d.name ? `${d.el} (${d.name})` : d.el;
  return `[structure] ${where} \xB7 ${d.kind}: expected ${fmtVal3(d.expected)}, got ${fmtVal3(d.actual)}`;
}
function buildReviewResponse(structure, rubric, opts = {}, director) {
  const accept = opts.accept ?? DEFAULT_REVIEW_ACCEPT;
  const maxFixes = opts.maxFixes ?? 8;
  const s = scoreOf(structure);
  const structureScore = s.score;
  const designScore = rubric.overall;
  const directorScore = director?.score;
  const w = director ? { s: 0.4, d: 0.3, v: 0.3 } : { s: STRUCTURE_WEIGHT, d: DESIGN_WEIGHT, v: 0 };
  const score = Math.round((w.s * structureScore + w.d * designScore + w.v * (directorScore ?? 0)) * 10) / 10;
  const rubricHasError = rubric.issues.some((i) => i.severity === "error");
  const directorHasError = (director?.issues ?? []).some((i) => i.severity === "error");
  const done = structure.ok && !rubricHasError && !directorHasError && score >= accept;
  const structureFixes = structure.deltas.filter((d) => d.severity !== "info" && d.kind !== "missing-in-pds").map((d) => ({ severity: d.severity, text: fmtDelta(d) }));
  const designFixes = rubric.issues.map((i) => ({
    severity: i.severity,
    text: `[design/${i.dimension}]${i.el ? ` ${i.el}` : ""} ${i.message} \u2192 ${i.fix}`
  }));
  const directorFixes = (director?.issues ?? []).map((i) => ({
    severity: i.severity,
    text: `[director/${i.dimension}]${i.el ? ` ${i.el}` : ""} ${i.message} \u2192 ${i.fix}`
  }));
  const topFixes = [...structureFixes, ...designFixes, ...directorFixes].sort((a, b) => RANK2[a.severity] - RANK2[b.severity]).slice(0, maxFixes).map((f) => f.text);
  const pct = score.toFixed(1);
  const weakest = rubric.dimensions.slice().sort((a, b) => a.score - b.score)[0];
  const directorIsWeakest = director !== void 0 && directorScore !== void 0 && directorScore < structureScore && directorScore < designScore;
  let instruction;
  if (done) {
    const axes = director ? `structure ${structureScore.toFixed(1)}%, design ${designScore}%, director ${directorScore}%` : `structure ${structureScore.toFixed(1)}%, design ${designScore}%`;
    instruction = `Ship-ready \u2014 ${pct}% (${axes}). Built every key node cleanly and the design holds up. Stop.`;
  } else if (structure.matched === 0) {
    instruction = `Nothing to review yet (structure 0%). Emit the design, pass EmitResult.ids so the built nodes can be joined to what you authored, then call plumb_review again.`;
  } else {
    const it = opts.iteration ? `Iteration ${opts.iteration}: ` : "";
    const lead = !structure.ok ? `fix the ${s.errors} structural error(s)` : directorHasError ? `resolve the director's failing issue(s) \u2014 the render looks broken` : rubricHasError ? `resolve the failing ${weakest ? weakest.label.toLowerCase() : "design"} issue(s)` : directorIsWeakest ? `address the director's critique (${directorScore}%)${director?.verdict ? `: "${director.verdict}"` : ""}` : `lift ${weakest ? weakest.label.toLowerCase() : "the weakest dimension"} (${weakest ? weakest.score : "?"}%)`;
    instruction = `${it}${pct}% \u2014 ${lead} from topFixes, re-apply (mode:"sync" keeps plumbKey), and review again.`;
  }
  return {
    score,
    structureScore,
    designScore,
    done,
    bar: renderBar(score),
    iteration: opts.iteration,
    matched: structure.matched,
    importantMatched: s.importantMatched,
    importantTotal: s.importantTotal,
    structureErrors: s.errors,
    structureWarns: s.warns,
    dimensions: rubric.dimensions.map((d) => ({
      id: d.id,
      label: d.label,
      score: d.score,
      weight: d.weight
    })),
    topFixes,
    instruction,
    ...director ? { directorScore, directorVerdict: director.verdict ?? "" } : {}
  };
}

// src/tools/review.ts
var DESCRIPTION10 = 'Self-critique the design you just emitted into Figma, and coach the refine loop \u2014 the write-direction mirror of plumb_fit. It scores up to three axes:\n  \u2022 STRUCTURE \u2014 re-serializes the built Figma nodes to a PDS and diffs them against the PDS your DSL compiled to (did emit build what you authored?). Uses the same verify engine as plumb_verify/plumb_fit. The two docs are joined by `ids` = EmitResult.ids (authored el \u2192 Figma node id) \u2014 pass it or the diff can\'t line up.\n  \u2022 DESIGN \u2014 a deterministic rubric over hierarchy, spacing rhythm, contrast (WCAG AA), alignment, type-scale, and professional-vs-templated polish. Failing contrast is an error and blocks `done`.\n  \u2022 DIRECTOR (optional, `director: {score, verdict?, issues?}`) \u2014 a vision creative-director grade of the rendered screenshot: visual balance, focal flow, image composition/crop, optical spacing, and the "designed vs generated" gestalt a deterministic pass over the PDS can\'t see. There is NO server-side vision call and NO API key needed here \u2014 YOU (the calling agent) already have vision, so YOU grade the screenshot yourself and pass your verdict in. The loop: call plumb_screenshot on the emitted rootId, look at the PNG, grade it as a demanding creative director (any response with no `director` input echoes the exact grading criteria + output shape at the end of its `instruction`), then call plumb_review again passing `director: { score, verdict, issues }`. With the director present, weights reshuffle to structure 0.4 / design 0.3 / director 0.3 and a director error-severity issue blocks `done` too, so the bar is harder (and more honest) to clear.\nReturns: score (0\u2013100 blended), done, bar, topFixes (error-first across all active axes, director fixes tagged `[director/<dim>]`), instruction, dimensions[] (per-rubric-dimension sub-scores), directorScore/directorVerdict when graded, plus the raw structural deltas + coverage. Provide the authored PDS inline via `authored`, or a path to it via `authoredPath` (the JSON plumb_apply writes). Point id/name/url at the emitted root (EmitResult.rootId). Canonical loop: plumb_design \u2192 repeat[ plumb_screenshot (rootId) \u2192 grade it yourself \u2192 plumb_review(..., director: {score, issues}) \u2192 if !done, apply topFixes and plumb_design(mode:"sync") ] until done or an iteration cap. If done=false, apply topFixes, re-apply the DSL (mode:"sync" keeps plumbKey), and call plumb_review again \u2014 the score should climb.';
var briefSchema = z12.object({
  typeSizes: z12.array(z12.number()).optional(),
  fonts: z12.array(z12.string()).optional(),
  spacingBase: z12.number().optional(),
  note: z12.string().optional()
}).optional();
function coerceAuthored(raw, where) {
  if (typeof raw === "string") {
    try {
      raw = JSON.parse(raw);
    } catch {
    }
  }
  if (!raw || typeof raw !== "object") {
    throw new PlumbError(
      `The authored PDS from ${where} is not an object.`,
      "Pass the PdsDocument the compiler produced (it has { root, nodes, tokens })."
    );
  }
  const doc = raw;
  if (typeof doc.root !== "string" || !doc.nodes || !doc.tokens) {
    throw new PlumbError(
      `The authored PDS from ${where} is missing root/nodes/tokens.`,
      "Provide the full PdsDocument that plumb_apply compiled and emitted."
    );
  }
  return raw;
}
function registerPlumbReview(server) {
  server.registerTool(
    "plumb_review",
    {
      title: "Plumb \xB7 review",
      description: DESCRIPTION10,
      inputSchema: {
        id: z12.string().optional().describe("Emitted root node id (EmitResult.rootId)."),
        name: z12.string().optional().describe("Screen name (plugin path)."),
        fileKey: z12.string().optional().describe("File key (REST path)."),
        url: z12.string().optional().describe("Paste a full Figma URL \u2014 fileKey and node-id are auto-extracted."),
        authored: z12.any().optional().describe("The authored PdsDocument (what the DSL compiled to). Inline alternative to authoredPath."),
        authoredPath: z12.string().optional().describe("Path to a JSON file holding the authored PdsDocument (written by plumb_apply)."),
        ids: z12.record(z12.string(), z12.string()).optional().describe("EmitResult.ids \u2014 authored el \u2192 Figma node id. The join key for the structural diff."),
        brief: briefSchema.describe("Optional design intent (type scale, fonts, spacing grid) to grade against."),
        accept: z12.number().min(0).max(100).optional().describe(`Blended score at which to stop. Default ${DEFAULT_REVIEW_ACCEPT}.`),
        iteration: z12.number().int().positive().optional().describe("Which refine pass this is (1, 2, 3\u2026). Informational \u2014 sharpens the coaching."),
        tolerances: z12.object({
          px: z12.object({ ok: z12.number(), warn: z12.number() }).optional(),
          color: z12.object({ ok: z12.number(), warn: z12.number() }).optional()
        }).optional().describe("Override default structural thresholds."),
        depth: z12.number().int().positive().optional().describe("PDS depth to re-serialize. Default 12."),
        director: z12.object({
          score: z12.number().min(0).max(100),
          verdict: z12.string().optional(),
          issues: z12.array(
            z12.object({
              dimension: z12.enum([
                "hierarchy",
                "spacing",
                "contrast",
                "alignment",
                "type-scale",
                "polish"
              ]),
              severity: z12.enum(["error", "warn", "info"]),
              el: z12.string().optional(),
              message: z12.string(),
              fix: z12.string()
            })
          ).optional()
        }).optional().describe(
          "YOUR OWN vision grade of the rendered screenshot \u2014 no server-side model call, no API key. Look at the PNG from plumb_screenshot, grade it yourself as a demanding creative director (see directorGuidance in a no-director response for the exact criteria), and pass { score, verdict?, issues? } here. When present, reweights the blend to structure 0.4 / design 0.3 / director 0.3 and a director error blocks `done`."
        )
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false }
    },
    async (args) => {
      try {
        let authored;
        if (args.authored !== void 0) {
          authored = coerceAuthored(args.authored, "the `authored` argument");
        } else if (args.authoredPath) {
          if (!existsSync7(args.authoredPath)) {
            throw new PlumbError(
              `authoredPath does not exist: ${args.authoredPath}`,
              "Pass the JSON path plumb_apply wrote, or the authored PDS inline via `authored`."
            );
          }
          let parsed;
          try {
            parsed = JSON.parse(readFileSync8(args.authoredPath, "utf8"));
          } catch (e) {
            throw new PlumbError(
              `Could not parse authoredPath as JSON: ${e.message}`,
              "Ensure the file holds the PdsDocument plumb_apply serialized."
            );
          }
          authored = coerceAuthored(parsed, args.authoredPath);
        } else {
          throw new PlumbError(
            "plumb_review needs the authored design to compare against.",
            "Pass the PdsDocument the compiler produced via `authored`, or its path via `authoredPath`."
          );
        }
        const depth = args.depth ?? 12;
        const target = await resolveVerifyTarget(args, depth);
        if (target.kind === "ambiguous") {
          return ok({
            ambiguous: true,
            matches: target.matches,
            next: "Several screens share that name. Each row shows `page` and `box` (w\xD7h) \u2014 pick the emitted one and re-call plumb_review with its `id`."
          });
        }
        const { source, screen, pds: built } = target;
        const remap = remapBuiltToAuthored(built, args.ids);
        const rendered = applyRemap(pdsToRendered(built), remap);
        const tolerances = {
          px: { ...DEFAULT_TOLERANCES.px, ...args.tolerances?.px ?? {} },
          color: { ...DEFAULT_TOLERANCES.color, ...args.tolerances?.color ?? {} }
        };
        const structure = verifyAgainst(authored, rendered, tolerances);
        const rubric = critiqueDesign(built, args.brief);
        const director = args.director ? coerceDirectorVerdict(args.director) : void 0;
        const review = buildReviewResponse(
          structure,
          rubric,
          { accept: args.accept, iteration: args.iteration },
          director
        );
        emitStudio({
          kind: "fit",
          tool: "plumb_review",
          screen,
          score: review.score,
          done: review.done,
          matched: review.matched,
          importantMatched: review.importantMatched,
          importantTotal: review.importantTotal,
          deltas: structure.deltas.slice(0, 12).map((d) => ({ el: d.el, kind: d.kind, severity: d.severity })),
          summary: `review ${review.score.toFixed(1)}% \u2014 struct ${review.structureScore.toFixed(1)} / design ${review.designScore}` + (director ? ` / director ${review.directorScore}` : "") + (review.done ? " \xB7 done \u2713" : "")
        });
        if (!director) {
          review.instruction = `${review.instruction} For a sharper (and stricter) review, screenshot the emitted rootId with plumb_screenshot, grade it yourself as a demanding creative director, then call plumb_review again passing director:{score,verdict,issues}. Grading criteria: ${directorGuidance()}`;
        }
        if (remap.size === 0 && args.ids === void 0) {
          review.instruction = "No `ids` given \u2014 built and authored nodes were joined by raw el, which usually won't line up. Pass EmitResult.ids for a real structural diff. " + review.instruction;
        }
        return ok({
          source,
          screen,
          score: review.score,
          structureScore: review.structureScore,
          designScore: review.designScore,
          directorScore: review.directorScore,
          directorVerdict: review.directorVerdict,
          done: review.done,
          bar: review.bar,
          iteration: review.iteration,
          matched: review.matched,
          importantMatched: review.importantMatched,
          importantTotal: review.importantTotal,
          structureErrors: review.structureErrors,
          structureWarns: review.structureWarns,
          dimensions: review.dimensions,
          topFixes: review.topFixes,
          instruction: review.instruction,
          rubric: {
            overall: rubric.overall,
            dimensions: rubric.dimensions,
            issues: rubric.issues,
            directorIssues: director?.issues ?? []
          },
          deltas: structure.deltas,
          coverage: structure.coverage
        });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/screenshot.ts
import {
  mkdirSync as mkdirSync4,
  renameSync as renameSync2,
  copyFileSync as copyFileSync2,
  statSync as statSync3,
  unlinkSync as unlinkSync2,
  existsSync as existsSync8
} from "fs";
import { isAbsolute, join as join10, parse } from "path";
import { z as z13 } from "zod";
var DESCRIPTION11 = "Render a Figma screen or node to PNG (or JPG) and save it locally; returns the file path. Use this as a visual reference while building UI from the PDS \u2014 and later as the source for plumb_verify. Plugin path; needs the Plumb plugin paired.";
function slug3(s) {
  return s.toLowerCase().normalize("NFKD").replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 60) || "screenshot";
}
function uniquePath(path) {
  if (!existsSync8(path)) return path;
  const { dir, name, ext } = parse(path);
  for (let i = 2; i < 1e3; i++) {
    const next = join10(dir, `${name}-${i}${ext}`);
    if (!existsSync8(next)) return next;
  }
  return path;
}
function pickPath(root, ext, nodeName, nodeId2, out) {
  if (out) {
    const explicit = isAbsolute(out) ? out : join10(root, out);
    const parsed = parse(explicit);
    const fixed = parsed.ext.toLowerCase() === `.${ext}` ? explicit : `${explicit}.${ext}`;
    return uniquePath(fixed);
  }
  const idSlug = slug3(nodeId2);
  const nameSlug = slug3(nodeName);
  const file = `${nameSlug}-${idSlug}.${ext}`;
  return uniquePath(join10(root, file));
}
function registerPlumbScreenshot(server) {
  server.registerTool(
    "plumb_screenshot",
    {
      title: "Plumb \xB7 screenshot",
      description: DESCRIPTION11,
      inputSchema: {
        id: z13.string().optional().describe("Node id to render."),
        name: z13.string().optional().describe("Screen name \u2014 resolved against the file."),
        scale: z13.number().positive().max(4).optional().describe("Render scale. Default 2."),
        format: z13.enum(["PNG", "JPG"]).optional().describe("Output format. Default PNG."),
        out: z13.string().optional().describe(
          "Output path or filename. Absolute paths are honoured as-is; bare filenames are placed under the screenshots directory. Defaults to '<name>-<id>.<ext>', auto-suffixed on collision."
        )
      },
      annotations: { readOnlyHint: false, idempotentHint: true, openWorldHint: false }
    },
    async (args) => {
      try {
        if (!bridge.paired) {
          throw new PlumbError(
            "No Figma plugin is paired.",
            "plumb_screenshot uses the plugin path \u2014 run the Plumb plugin in Figma and click 'Pair with Plumb'."
          );
        }
        const resolved = resolveScreen(args.id, args.name);
        if ("ambiguous" in resolved) {
          return ok({
            ambiguous: true,
            matches: formatScreenMatches(resolved.ambiguous),
            next: "Several screens share that name. Each row shows `page` and `box` (w\xD7h) \u2014 pick the one you want and re-call plumb_screenshot with its `id`."
          });
        }
        const { path: tempPath, format, nodeName, error } = await requestScreenshot(
          resolved.id,
          args.scale,
          args.format
        );
        if (error) {
          throw new PlumbError(
            `The plugin could not render the screenshot: ${error}`,
            "Retry; if it persists, the node may have been deleted or renamed."
          );
        }
        if (!tempPath) {
          throw new PlumbError(
            "The plugin reported a screenshot but no image bytes arrived.",
            "Re-run the Plumb plugin and retry; if it persists, file a bug."
          );
        }
        const root = process.env.PLUMB_SCREENSHOTS_DIR ?? join10(process.cwd(), "plumb-screenshots");
        const ext = format.toLowerCase();
        const path = pickPath(
          root,
          ext,
          nodeName || screenName(resolved.id) || resolved.id,
          resolved.id,
          args.out
        );
        mkdirSync4(parse(path).dir, { recursive: true });
        try {
          renameSync2(tempPath, path);
        } catch {
          copyFileSync2(tempPath, path);
          try {
            unlinkSync2(tempPath);
          } catch {
          }
        }
        const bytes = statSync3(path).size;
        return ok({
          source: "plugin",
          screen: nodeName,
          path,
          format,
          bytes,
          scale: args.scale ?? 2,
          next: "Use this image as the visual reference of the design \u2014 compare it against what you build."
        });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/search.ts
import { z as z14 } from "zod";
var DESCRIPTION12 = "Find nodes across the file by name and/or type \u2014 'where is the primary button?', 'every TEXT layer named Title', etc. Returns matches with id, name, type, page, and size. Drill into one with plumb_node, or pull its asset with plumb_assets. Plugin path; needs the Plumb plugin paired.";
function registerPlumbSearch(server) {
  server.registerTool(
    "plumb_search",
    {
      title: "Plumb \xB7 search",
      description: DESCRIPTION12,
      inputSchema: {
        query: z14.string().optional().describe("Substring match on node name (case-insensitive)."),
        type: z14.string().optional().describe('Filter by node type, e.g. "TEXT", "FRAME", "INSTANCE", "VECTOR".'),
        page: z14.string().optional().describe(
          "Filter to a single Figma page by name (case-insensitive, substring-friendly). Drops noise on multi-page files."
        )
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false }
    },
    async (args) => {
      try {
        if (!bridge.paired) {
          throw new PlumbError(
            "No Figma plugin is paired.",
            "plumb_search uses the plugin path \u2014 run the Plumb plugin in Figma and click 'Pair with Plumb'."
          );
        }
        if (!args.query && !args.type) {
          throw new PlumbError(
            "Provide a `query` or a `type`.",
            'e.g. plumb_search({ query: "button" }) or plumb_search({ type: "TEXT" }).'
          );
        }
        const { matches, error } = await requestSearch(args.query, args.type);
        if (error) {
          throw new PlumbError(
            `The plugin could not search: ${error}`,
            "Retry; if it persists, re-run the Plumb plugin in Figma."
          );
        }
        const pageFilter = args.page?.trim().toLowerCase();
        const filtered = pageFilter ? matches.filter((m) => (m.page ?? "").toLowerCase().includes(pageFilter)) : matches;
        return ok({
          source: "plugin",
          count: filtered.length,
          ...pageFilter ? { page: args.page } : {},
          matches: filtered,
          next: filtered.length ? "Drill into one with plumb_node({ id }) or pull its asset with plumb_assets({ ids: [id] })." : "No matches. Try a different query or type."
        });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/selection.ts
import { z as z15 } from "zod";
var DESCRIPTION13 = "Extract whatever the user currently has selected in Figma, via the paired Plumb plugin \u2014 no file key, no token, no rate limit. Returns the same compact PDS as plumb_node. Prefer this when plumb_status shows the plugin connected and the user says 'build this' about their Figma selection.";
function registerPlumbSelection(server) {
  server.registerTool(
    "plumb_selection",
    {
      title: "Plumb \xB7 selection",
      description: DESCRIPTION13,
      inputSchema: {
        depth: z15.number().int().min(1).max(12).optional().describe("Levels to disclose. Default 3."),
        notes: z15.boolean().optional().describe("Include human-readable notes per node."),
        maxTokens: z15.number().int().positive().optional().describe("Soft token budget; fit-to-budget reduces depth to fit.")
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false }
    },
    async (args) => {
      try {
        if (!bridge.paired) {
          throw new PlumbError(
            "No Figma plugin is paired.",
            "Open your file in Figma, run the Plumb plugin, and click 'Pair with Plumb' in its panel."
          );
        }
        if (!bridge.selection) {
          throw new PlumbError(
            "The Plumb plugin is paired, but nothing is selected in Figma.",
            "Select a frame in Figma \u2014 the plugin streams your selection automatically."
          );
        }
        const file = {
          document: bridge.selection.doc,
          fileName: bridge.selection.fileName,
          version: `plugin-${bridge.selection.receivedAt}`
        };
        const pds = normalizeToBudget(file, args.depth ?? 3, args.maxTokens, {
          notes: args.notes
        });
        return ok({ ...pds, source: "plugin", selection: bridge.selection.nodeName });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/source.ts
import { createHash as createHash2 } from "crypto";
import { mkdirSync as mkdirSync5, writeFileSync as writeFileSync5 } from "fs";
import { join as join11 } from "path";
import { z as z16 } from "zod";
var DESCRIPTION14 = "Source design assets from the open web \u2014 icons, photos, avatars, illustrations \u2014 for the write direction. Queries every provider in parallel (keyless-first: Iconify, Lorem Picsum, DiceBear always work; Unsplash / Pexels / Pixabay activate when their API key is in env) and ranks the results with one scoring function, including pick-the-right-icon-pack so a design uses ONE consistent icon family. Two modes:\n  \u2022 search (default) \u2014 return ranked candidates (metadata only, no bytes). Cheap; use to see what's available and which icon pack was locked.\n  \u2022 fetch \u2014 resolve the best match, download its bytes to a local folder, and return the path (+ inline SVG / small data: URI). Never fails: a total miss degrades to a deterministic placeholder.";
var KINDS = [
  "icon",
  "photo",
  "illustration",
  "avatar",
  "pattern",
  "font",
  "mockup",
  "logo",
  "generated"
];
var STYLES = [
  "flat",
  "line",
  "outline",
  "filled",
  "duotone",
  "3d",
  "photo",
  "illustration",
  "hand-drawn",
  "geometric",
  "gradient"
];
var WEIGHTS2 = ["thin", "light", "regular", "medium", "bold"];
function slug4(s) {
  return s.toLowerCase().normalize("NFKD").replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 48) || "asset";
}
function writeSourced(name, r) {
  const root = process.env.PLUMB_ASSETS_DIR ?? join11(process.cwd(), "plumb-assets");
  const dir = join11(root, "sourced");
  mkdirSync5(dir, { recursive: true });
  const hash = createHash2("sha1").update(r.bytes).digest("hex").slice(0, 10);
  const file = `${slug4(name)}-${hash}.${r.ext ?? "bin"}`;
  const full = join11(dir, file);
  writeFileSync5(full, Buffer.from(r.bytes));
  return full;
}
var INLINE_MAX_BYTES2 = 64 * 1024;
function inlineSource(r) {
  if (!r.bytes) return void 0;
  if (r.ext === "svg") return new TextDecoder().decode(r.bytes);
  if (r.bytes.byteLength > INLINE_MAX_BYTES2) return void 0;
  return `data:${r.mime ?? "application/octet-stream"};base64,${Buffer.from(r.bytes).toString("base64")}`;
}
function registerPlumbSource(server) {
  server.registerTool(
    "plumb_source",
    {
      title: "Plumb \xB7 source",
      description: DESCRIPTION14,
      inputSchema: {
        query: z16.string().describe("What to look for, e.g. 'rocket launch' or 'shopping cart'."),
        mode: z16.enum(["search", "fetch"]).optional().describe("`search` (default) returns ranked candidates; `fetch` downloads the best."),
        kind: z16.enum(KINDS).optional().describe("Constrain to one asset kind."),
        style: z16.array(z16.enum(STYLES)).optional().describe("Preferred style tags (ranking hint)."),
        weight: z16.enum(WEIGHTS2).optional().describe("Icon weight preference."),
        aspect: z16.number().optional().describe("Target width/height ratio (ranking hint)."),
        minWidth: z16.number().optional().describe("Minimum acceptable pixel width."),
        palette: z16.array(z16.string()).optional().describe("Brand hex colors \u2014 recolors monotone SVG icons."),
        seed: z16.string().optional().describe("Deterministic seed for avatars / placeholders."),
        provider: z16.string().optional().describe("Restrict to one provider id (iconify, unsplash, pexels, pixabay, dicebear, picsum, \u2026)."),
        w: z16.number().optional().describe("Desired width (photos / placeholders)."),
        h: z16.number().optional().describe("Desired height (photos / placeholders)."),
        limit: z16.number().int().positive().optional().describe("Max candidates in search mode. Default 16."),
        inline: z16.boolean().optional().describe("fetch mode: also return the bytes inline (SVG markup / small data: URI).")
      },
      annotations: { readOnlyHint: false, idempotentHint: false, openWorldHint: true }
    },
    async (args) => {
      try {
        const spec = {
          query: args.query,
          kind: args.kind,
          style: args.style,
          weight: args.weight,
          aspect: args.aspect,
          minWidth: args.minWidth,
          palette: args.palette,
          seed: args.seed,
          provider: args.provider,
          w: args.w,
          h: args.h
        };
        const mode = args.mode ?? "search";
        if (mode === "search") {
          const res = await searchAssets(spec, { limit: args.limit });
          emitStudio({
            kind: "tool",
            tool: "plumb_source",
            summary: `sourced ${res.candidates.length} candidate(s) for "${args.query}"`
          });
          return ok({
            mode: "search",
            query: args.query,
            iconPack: res.iconPack,
            providersRun: res.providersRun,
            providersDropped: res.providersDropped,
            count: res.candidates.length,
            candidates: res.candidates.map((c) => ({
              id: c.id,
              provider: c.provider,
              kind: c.kind,
              title: c.title,
              style: c.style,
              w: c.w,
              h: c.h,
              url: c.url,
              license: c.license,
              attribution: c.attribution,
              score: c.score
            })),
            next: res.candidates.length === 0 ? "No candidates. Broaden the query, drop `provider`, or (for photos) set an UNSPLASH_ACCESS_KEY / PEXELS_API_KEY / PIXABAY_API_KEY in env \u2014 Picsum still serves a keyless fallback." : "Call plumb_source again with `mode: 'fetch'` (same args) to download the top match, or narrow with `provider`/`style`."
          });
        }
        const r = await resolveAsset(spec, { limit: args.limit });
        if (!r.bytes) {
          return ok({
            mode: "fetch",
            query: args.query,
            resolved: false,
            placeholder: true,
            kind: r.kind,
            next: "Could not fetch any bytes (network or every provider missed). Retry, or pass a different `query`/`kind`."
          });
        }
        const path = writeSourced(args.query, r);
        const source = args.inline ? inlineSource(r) : void 0;
        emitStudio({
          kind: "tool",
          tool: "plumb_source",
          summary: `fetched ${r.kind} for "${args.query}"${r.placeholder ? " (placeholder)" : ""}`
        });
        return ok({
          mode: "fetch",
          query: args.query,
          resolved: true,
          placeholder: r.placeholder ?? false,
          provider: r.provider,
          kind: r.kind,
          ext: r.ext,
          mime: r.mime,
          path,
          bytes: r.bytes.byteLength,
          w: r.w,
          h: r.h,
          url: r.url,
          scaleMode: r.scaleMode,
          license: r.license,
          attribution: r.attribution,
          ...r.inlineSvg ? { inlineSvg: r.inlineSvg } : {},
          ...source !== void 0 ? { source } : {},
          next: "Use `path` when building, or pass these bytes to the plugin inbound channel (stageInboundAsset \u2192 GET /asset/:key) during apply. Honor `attribution` where the license requires it."
        });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/keylegend.ts
var KEY_LEGEND = {
  el: 'stable node handle \u2014 tag rendered DOM with data-plumb-id="<el>"',
  id: "raw Figma node id \u2014 pass to plumb_node to drill into this node",
  box: "{ w, h } size in CSS px",
  pos: "{ x, y } position relative to parent (omitted when parent has auto-layout)",
  "layout.flow": "flex-direction \u2014 'row' or 'col'",
  "layout.gap": "flex gap in px (Figma itemSpacing)",
  "layout.pad": "padding [top, right, bottom, left] in px",
  "layout.justify": "justify-content (omitted when flex-start)",
  "layout.align": "align-items (omitted when flex-start)",
  "layout.wrap": "flex-wrap is on",
  layout: "auto-layout config object \u2014 OR a $lN ref into tokens.layout when the same layout repeats across siblings (resolve via the table; same dedup pattern as $cN colors)",
  fill: "background \u2014 token ref $cN into tokens.color, or 'gradient' / 'image'",
  stroke: "border colour \u2014 token ref $cN into tokens.color",
  strokeW: "border width in px",
  radius: 'border-radius \u2014 token ref $rN into tokens.radius (value is px or "full" for pill/circle), or per-corner [tl,tr,br,bl]',
  shadow: "compact CSS box-shadow string for a single shadow (dominant case)",
  effects: "structured stack of drop/inner shadows, layer-blur, background-blur \u2014 read this on glass / multi-shadow surfaces instead of `shadow`. May arrive as a $eN ref into tokens.effects when the same elevation stack repeats across surfaces.",
  backdropFilter: "CSS backdrop-filter shorthand (e.g. 'blur(24px)') when the node has a Figma background-blur",
  fills: "structured fill stack \u2014 solid / linear-gradient / radial-gradient / image with assetId. Emitted whenever the compact `fill` string would be lossy (multi-fill, gradient, image). Render in CSS as comma-separated layers. May arrive as a $fN ref into tokens.fills for repeating gradient/solid stacks (image stacks stay inline).",
  text: "type style \u2014 token ref $tN into tokens.text ('weight size/lh family')",
  textDecoration: "'underline' or 'line-through' on TEXT nodes (Figma STRIKETHROUGH/UNDERLINE)",
  chars: "literal text content of a TEXT node",
  opacity: "0..1, omitted when 1",
  clip: "overflow is clipped",
  component: "mainComponent id (INSTANCE nodes)",
  assetId: "Figma asset id when this node renders an image \u2014 tag DOM with data-plumb-asset for verify",
  path: 'globally-unique dotted ancestor path; tag deeply-nested DOM with data-plumb-id="<path>" when bare `el` would collide',
  motion: "Figma prototype reactions on this node \u2014 list of { trigger, kind, duration, easing, target }",
  iconHint: "inferred icon meaning for small image/vector nodes \u2014 derived from sibling TEXT labels and ancestor names. Use it to swap bitmap icons for your codebase's line-icon library without reading pixels.",
  children: "child `el` handles included in this response",
  more: "this many children were NOT included \u2014 call plumb_node on `id` to expand",
  vectorPath: "inline SVG path `d` string for small icons \u2014 OR a $vN ref into tokens.vector when the same icon shape repeats (raw `d` strings never start with $, so the prefix is unambiguous)",
  props: "component instance property overrides \u2014 OR a $pN ref into tokens.props when the same prop set repeats across instances",
  "tokens.layout / tokens.effects / tokens.fills / tokens.vector / tokens.props": "v0.10 compound token tables \u2014 repeated structured values (layouts, effect stacks, fill stacks, vector paths, instance prop maps) emitted once and referenced by $-id from the node tree. Resolve $xN refs by looking up the matching table.",
  "tokens.meta.counts": "per-ref hit counts (\u22652 only) \u2014 concrete evidence of dedup, useful for sizing 'tokens before vs after' on a real screen",
  rotation: "CSS-shaped rotation in degrees (clockwise). Omitted at 0\xB0.",
  blend: "mix-blend-mode keyword (multiply / overlay / screen / \u2026). Omitted when PASS_THROUGH or NORMAL.",
  smooth: "Figma squircle factor (0..1) \u2014 apply via the `--corner-smoothing` system or @container-style polyfill. Omitted at 0.",
  textGrow: "text auto-resize hint \u2014 'h' = height grows, 'wh' = both, 'trunc' = ellipsize. Omitted when fixed.",
  constraints: "pinning for non-auto-layout children \u2014 { h?: left/right/center/stretch/scale, v?: top/bottom/center/stretch/scale }. Dropped on flex children where CSS pinning is meaningless.",
  sizingMin: "per-axis size floor \u2014 { w?, h? } in px. Kicks in with FILL/HUG sizing to prevent collapse below the minimum.",
  sizingMax: "per-axis size ceiling \u2014 { w?, h? } in px. Pairs with sizingMin for clamped responsive boxes.",
  "component (object form)": "{ id, variant?: 'Key=Val,Key=Val' } \u2014 variant-aware mainComponent identity. Lets agents route to the right codebase variant without re-parsing `props`.",
  "chars (run array)": "mixed-style text \u2014 array of { t, s?, c?, d? } where t is the substring, s is a $tN style override, c is a $cN colour override, d is 'underline'/'line-through'. Omit overrides on runs that match the dominant style/colour/decoration on the node.",
  "fills[].crop": "2\xD73 affine matrix Figma applies to an IMAGE paint to position/crop the source. Pair with `fills[].rotation` for rotated photos.",
  "motion.overlay": "{ pos?, background? } when the action opens an overlay. Without this an agent defaults to a centred modal; with it, agents reconstruct top-pinned sheets, custom-position drawers, dimmed backdrops correctly.",
  "repeat (array form)": "v0.10 Phase 4 \u2014 when a parent has multiple repeating clusters ('Header + 6 rows + Spacer + 4 cards'), repeat is an array, one { template, data } per run. Normalise with `Array.isArray(parent.repeat) ? parent.repeat : [parent.repeat]`."
};

// src/tools/status.ts
var DESCRIPTION15 = "Plumb's status and self-description \u2014 call this FIRST. Reports which data paths are available (the paired Figma plugin, and/or the REST token), how many screens the plugin sees, the compact-key legend for reading every PDS response, and the token budget. Needs no Figma access. Plumb is the rate-limit-free, plan-free alternative to Figma's official Dev Mode MCP and Framelink (figma-developer-mcp) \u2014 reach for it when the official MCP is plan-gated, exceeds the 25k token cap, or when REST returns 429.";
function registerPlumbStatus(server) {
  server.registerTool(
    "plumb_status",
    {
      title: "Plumb \xB7 status",
      description: DESCRIPTION15,
      inputSchema: {},
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false }
    },
    async () => {
      const restConfigured = Boolean(
        process.env.FIGMA_TOKEN ?? process.env.FIGMA_ACCESS_TOKEN
      );
      const screenCount = bridge.inventory ? flatScreens().length : 0;
      return ok({
        server: { name: SERVER_NAME, version: SERVER_VERSION, milestone: "M3" },
        plugin: {
          connected: bridge.paired,
          bridgePort: bridge.port,
          pluginVersion: bridge.pluginVersion,
          fileName: bridge.inventory?.fileName ?? null,
          screens: screenCount,
          selection: bridge.selection ? bridge.selection.nodeName : null,
          note: bridge.paired ? `Plugin paired \u2014 ${screenCount} screen(s) available. Use plumb_outline to list them, then plumb_node / plumb_assets by id or name (no token, no rate limit).` : bridge.port !== null ? "Bridge is listening, but no plugin has paired. In Figma, run the Plumb plugin and click 'Pair with Plumb'." : "The plugin bridge is not running; the REST tools still work."
        },
        rest: {
          configured: restConfigured,
          note: restConfigured ? "FIGMA_TOKEN is set \u2014 the REST tools are ready." : "Set FIGMA_TOKEN to use the REST tools (figma.com \u2192 Settings \u2192 Security)."
        },
        tools: [
          "plumb_status",
          "plumb_outline",
          "plumb_node",
          "plumb_tokens",
          "plumb_selection",
          "plumb_assets",
          "plumb_screenshot",
          "plumb_describe",
          "plumb_search",
          "plumb_components",
          "plumb_verify",
          "plumb_fit",
          "plumb_query",
          "plumb_fig_outline",
          "plumb_fig_node",
          "plumb_design",
          "plumb_brand",
          "plumb_studio",
          "plumb_source",
          "plumb_review",
          "plumb_studio_start",
          "plumb_studio_kit",
          "plumb_studio_page"
        ],
        legend: KEY_LEGEND,
        budget: {
          mcpCeiling: 25e3,
          target: 1e4,
          note: "Keep responses within budget \u2014 drill with `depth` and `more`, or pass `maxTokens` to let fit-to-budget pick the depth."
        },
        next: bridge.paired ? "Call plumb_outline to see every screen, then plumb_node by id or name." : restConfigured ? "Call plumb_outline with a fileKey, then plumb_node on a screen id." : "Pair the Plumb plugin, or set FIGMA_TOKEN for the REST path."
      });
    }
  );
}

// src/tools/studio.ts
import { writeFileSync as writeFileSync6 } from "fs";
import { tmpdir as tmpdir4 } from "os";
import { join as join12 } from "path";
import { z as z17 } from "zod";
var DESCRIPTION16 = 'Direct a FULL landing page from a one-line brief and BUILD it into the paired Figma file \u2014 the headline director-completion tool. Given a brief (e.g. "a premium fintech dashboard"), Plumb discovers a diverse set of best-in-class reference sites, screenshots them live, synthesizes a coherent semantic palette from their computed CSS, then composes a real, brand-consistent page \u2014 nav, hero, features, a reference-imagery gallery, a content split, a cta, and a footer, with copy derived from the brief \u2014 and builds it through the same DSL -> PDS -> emit-plan write path plumb_design uses. Returns the picked references, the synthesized brand palette, the created node ids (keyed by authored element handle), and an authoredPath \u2014 feed both straight into plumb_review for the director critique loop. Requires the Plumb plugin to be paired.';
var STOPWORDS = /* @__PURE__ */ new Set([
  "a",
  "an",
  "the",
  "for",
  "with",
  "of",
  "and",
  "to",
  "that",
  "this",
  "your"
]);
function deriveName(brief) {
  const words = brief.split(/\s+/).filter(Boolean);
  const significant = words.filter((w) => !STOPWORDS.has(w.toLowerCase()));
  const picked = (significant.length ? significant : words).slice(0, 3);
  const name = picked.map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
  return name || "Studio";
}
function capFirst(s) {
  return s.length ? s.charAt(0).toUpperCase() + s.slice(1) : s;
}
function bareBrief(brief) {
  return brief.trim().replace(/^(a|an|the)\s+/i, "").replace(/[.!?]+$/, "");
}
function toEmitTarget3(pageName) {
  return { kind: "page", pageName };
}
function buildStudioDoc(input) {
  const { brief, pageName, refs, captures, brand } = input;
  if (refs.length === 0) {
    throw new Error("buildStudioDoc requires at least one reference.");
  }
  const assets = {};
  const keyByUrl = /* @__PURE__ */ new Map();
  for (const cap of captures) {
    const key = stageInboundAsset(cap.png, "png");
    assets[key] = { ext: "png", kind: "image", w: cap.width, h: cap.height };
    keyByUrl.set(cap.url, { key, w: cap.width, h: cap.height });
  }
  const resolver = {
    async resolve(spec) {
      const hit = keyByUrl.get(spec.query);
      if (hit) {
        return { assetId: hit.key, kind: "photo", w: hit.w, h: hit.h, scaleMode: "fill" };
      }
      return { kind: spec.kind ?? "photo" };
    }
  };
  const name = deriveName(brief);
  const bare = bareBrief(brief);
  const headline = capFirst(bare) || name;
  const sub = `A complete, on-brand landing page for ${bare || brief} \u2014 composed automatically from a single brief and ${refs.length} live reference site(s).`;
  const heroRef = refs[0];
  const splitRef = refs[refs.length - 1] ?? refs[0];
  const galleryCards = Array.from({ length: 3 }, (_, i) => {
    const ref = refs[i % refs.length];
    return { image: { query: ref.url, kind: "photo" }, title: ref.name, body: ref.why };
  });
  const features2 = [
    {
      icon: "lucide:sparkles",
      title: "Crafted for the brief",
      body: `Every section is composed to match the tone of "${bare || brief}" \u2014 not filled into a generic template.`
    },
    {
      icon: "lucide:palette",
      title: "One coherent palette",
      body: `Colours are extracted live from ${refs.length} reference site(s) and synthesized into a single semantic system.`
    },
    {
      icon: "lucide:layout-grid",
      title: "Built end to end",
      body: "Nav, hero, features, gallery, and a call to action \u2014 a full page in one director pass."
    }
  ];
  const contentChildren = [
    {
      type: "stack",
      name: "split",
      dir: "row",
      gap: 64,
      align: "center",
      w: "fill",
      children: [
        {
          type: "stack",
          name: "split-copy",
          dir: "col",
          gap: 16,
          grow: 1,
          children: [
            { type: "text", text: "How it comes together", style: "h2" },
            {
              type: "text",
              text: `Plumb studied ${refs.length} live reference site(s), synthesized a coherent palette from their computed CSS, then composed this page from semantic sections \u2014 nav, hero, features, gallery, and a call to action \u2014 all matched to the brief.`,
              style: "body",
              color: "@muted",
              w: "fill"
            }
          ]
        },
        {
          type: "image",
          src: { query: splitRef.url, kind: "photo" },
          w: "fill",
          h: 360,
          radius: 12,
          fit: "crop",
          grow: 1
        }
      ]
    }
  ];
  const sections = [
    {
      role: "nav",
      sticky: true,
      brand: { text: name },
      links: [
        { label: "Product", href: "#product" },
        { label: "Features", href: "#features" },
        { label: "Gallery", href: "#gallery" },
        { label: "About", href: "#about" }
      ],
      actions: [
        { type: "button", label: "Sign in", variant: "ghost" },
        { type: "button", label: "Get started", variant: "primary", icon: "lucide:arrow-right", iconPos: "right" }
      ]
    },
    {
      role: "hero",
      layout: "split-right",
      maxWidth: 1200,
      eyebrow: "INTRODUCING",
      headline,
      sub,
      actions: [
        { type: "button", label: "Get started", variant: "primary", size: "lg" },
        { type: "button", label: "See how it works", variant: "outline", size: "lg" }
      ],
      media: {
        type: "image",
        src: { query: heroRef.url, kind: "photo", role: "hero" },
        w: "fill",
        h: 420,
        radius: "lg"
      }
    },
    {
      role: "features",
      heading: "Everything you need",
      bg: "@surface",
      layout: "row",
      features: features2
    },
    {
      role: "card-grid",
      heading: "Inspired by the best",
      columns: 3,
      cards: galleryCards
    },
    { role: "content", children: contentChildren },
    {
      role: "cta",
      bg: "@primary",
      theme: "dark",
      headline: `Start with ${name}`,
      sub: "Free to explore \u2014 composed and ready to ship.",
      actions: [{ type: "button", label: "Get started", variant: "secondary", size: "lg" }]
    },
    {
      role: "footer",
      bg: "@surface",
      brand: { text: name },
      columns: [
        { title: "Product", links: [{ label: "Features" }, { label: "Gallery" }, { label: "Pricing" }] },
        { title: "Company", links: [{ label: "About" }, { label: "Contact" }] }
      ],
      legal: `\xA9 ${(/* @__PURE__ */ new Date()).getFullYear()} ${name}. Built with Plumb.`
    }
  ];
  const doc = {
    version: "1",
    meta: { name, description: brief },
    brand: {
      fonts: { heading: "Inter", body: "Inter" },
      colors: brand,
      type: {
        display: { size: 64, weight: 600, line: 1.05, font: "heading" },
        h1: { size: 40, weight: 600, line: 1.1, font: "heading" },
        h2: { size: 28, weight: 600, line: 1.2, font: "heading" },
        h3: { size: 20, weight: 600, line: 1.3, font: "heading" },
        body: { size: 16, weight: 400, line: 1.6, font: "body" },
        small: { size: 13, weight: 400, line: 1.5, font: "body" },
        label: { size: 12, weight: 600, line: 1.2, font: "body", transform: "upper" }
      },
      spacing: { xs: 4, sm: 8, md: 16, lg: 24, xl: 48 },
      radius: { sm: 8, md: 12, lg: 16, full: "full" }
    },
    pages: [{ name: pageName, width: 1440, bg: "@bg", sections }]
  };
  return { doc, resolver, assets };
}
function registerPlumbStudio(server) {
  server.registerTool(
    "plumb_studio",
    {
      title: "Plumb \xB7 studio (direct + build a full page)",
      description: DESCRIPTION16,
      inputSchema: {
        brief: z17.string().min(1).describe(
          `One-line description of what's being built, e.g. "a premium fintech dashboard". Drives reference discovery, palette, and copy.`
        ),
        count: z17.number().int().positive().optional().describe("How many reference sites to study + screenshot (default 4)."),
        references: z17.array(z17.string()).optional().describe(
          "Explicit reference URLs to include first \u2014 always studied ahead of the discovered catalogue."
        ),
        pageName: z17.string().optional().describe('Figma page to build the landing page onto (default "Studio").')
      },
      annotations: { readOnlyHint: false, idempotentHint: false, openWorldHint: false }
    },
    async (args) => {
      try {
        const brief = typeof args.brief === "string" ? args.brief.trim() : "";
        if (!brief) {
          throw new PlumbError(
            "A brief is required.",
            'Pass `brief` \u2014 a one-line description like "a premium fintech dashboard".'
          );
        }
        const count = args.count && args.count > 0 ? Math.floor(args.count) : 4;
        const references = Array.isArray(args.references) ? args.references : void 0;
        const pageName = typeof args.pageName === "string" && args.pageName.trim() ? args.pageName.trim() : "Studio";
        const refs = discoverReferences({ description: brief, references }, count);
        if (refs.length === 0) {
          throw new PlumbError(
            "Could not resolve any references for the brief.",
            "Add explicit `references` URLs or broaden the brief wording, then retry."
          );
        }
        if (!bridge.paired) {
          throw new PlumbError(
            "No Figma plugin is paired.",
            "plumb_studio writes through the plugin path \u2014 run the Plumb plugin in Figma and click 'Pair with Plumb'."
          );
        }
        emitStudio({
          kind: "log",
          tool: "plumb_studio",
          screen: pageName,
          summary: `capturing ${refs.length} reference site(s)\u2026`
        });
        const { captures, misses } = await captureUrls(
          refs.map((r) => r.url),
          { maxHeight: 2200, settleMs: 1600 }
        );
        for (const cap of captures) {
          emitStudio({
            kind: "log",
            tool: "plumb_studio",
            screen: pageName,
            summary: `captured ${cap.url} (${cap.width}\xD7${cap.height})`
          });
        }
        for (const miss of misses) {
          emitStudio({
            kind: "log",
            tool: "plumb_studio",
            screen: pageName,
            summary: `missed ${miss.url} \u2014 ${miss.error}`
          });
        }
        const seed = captures.find((c) => c.palette);
        if (!seed || !seed.palette) {
          throw new PlumbError(
            "No reference screenshot yielded a palette to synthesize a brand from.",
            "Check network access + that Chrome can reach the reference sites (PLUMB_CHROME), then retry."
          );
        }
        const brand = synthesizeBrand(seed.palette);
        emitStudio({
          kind: "log",
          tool: "plumb_studio",
          screen: pageName,
          summary: "composing landing page (nav, hero, features, gallery, cta, footer)\u2026"
        });
        const { doc, resolver, assets } = buildStudioDoc({
          brief,
          pageName,
          refs,
          captures,
          brand
        });
        let compiled;
        try {
          compiled = await compile(doc, { assets: resolver });
        } catch (e) {
          throw new PlumbError(
            `Compiling the studio page failed: ${e instanceof Error ? e.message : String(e)}`,
            "This is usually a malformed section/block. Retry, or simplify the brief."
          );
        }
        const planId = `studio-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const plan = lowerToEmitPlan(compiled.doc, {
          planId,
          target: toEmitTarget3(pageName),
          mode: "create",
          prune: false,
          reveal: true,
          components: compiled.components,
          assets
        });
        const { result, error } = await requestApplyDesign(
          plan,
          (p) => {
            emitStudio({
              kind: "log",
              tool: "plumb_studio",
              screen: pageName,
              summary: `${p.phase} ${p.done}/${p.total}${p.note ? ` \u2014 ${p.note}` : ""}`
            });
          }
        );
        if (error || !result) {
          throw new PlumbError(
            `The plugin could not build the studio page: ${error ?? "no result returned"}`,
            "Check that the plugin is still paired, then retry."
          );
        }
        const summary = `built "${doc.meta?.name ?? pageName}" \u2014 ${refs.length} reference(s), ${result.created} node(s)`;
        emitStudio({
          kind: "screen",
          tool: "plumb_studio",
          screen: pageName,
          summary
        });
        let authoredPath;
        try {
          authoredPath = join12(tmpdir4(), `plumb-authored-${planId}.json`);
          writeFileSync6(authoredPath, JSON.stringify(compiled.doc));
        } catch {
          authoredPath = void 0;
        }
        return ok({
          brief,
          references: refs.map((r) => ({ name: r.name, url: r.url })),
          captured: captures.length,
          misses,
          brand,
          rootId: result.rootId,
          ids: result.ids,
          authoredPath,
          warnings: compiled.warnings,
          summary,
          next: "Built. Run plumb_review with { id: rootId, ids, authoredPath } to score built-vs-authored and get the fix list."
        });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/studioFlow.ts
import { writeFileSync as writeFileSync7 } from "fs";
import { tmpdir as tmpdir5 } from "os";
import { join as join13 } from "path";
import { z as z18 } from "zod";

// src/studio/kit.ts
function primaryPill(labelPlaceholder) {
  return {
    type: "stack",
    name: "pill",
    dir: "row",
    align: "center",
    justify: "center",
    pad: [12, 20],
    radius: "full",
    bg: "@primary",
    children: [{ type: "text", text: labelPlaceholder, style: "label", color: "@onPrimary" }]
  };
}
function buttonComponent() {
  return {
    name: "Button",
    props: [{ name: "label", type: "text", default: "Get started" }],
    body: primaryPill("@prop.label")
  };
}
function featureCardComponent() {
  const iconChip = {
    type: "stack",
    name: "icon-chip",
    dir: "col",
    align: "center",
    justify: "center",
    w: 64,
    h: 64,
    bg: "@elevated",
    border: { color: "@border", width: 1 },
    radius: "full",
    children: [{ type: "icon", name: "@prop.icon", size: 30, color: "@primary" }]
  };
  const body = {
    type: "stack",
    name: "FeatureCard",
    dir: "col",
    gap: 12,
    pad: 24,
    w: "fill",
    bg: "@surface",
    border: { color: "@border", width: 1 },
    shadow: "lg",
    radius: 12,
    children: [
      iconChip,
      { type: "text", text: "@prop.title", style: "h3", w: "fill" },
      { type: "text", text: "@prop.body", style: "body", color: "@muted", w: "fill" }
    ]
  };
  return {
    name: "FeatureCard",
    props: [
      { name: "icon", type: "text", default: "lucide:sparkles" },
      { name: "title", type: "text", default: "Feature" },
      { name: "body", type: "text", default: "A concise description of the capability." }
    ],
    body
  };
}
function statCardComponent() {
  const body = {
    type: "stack",
    name: "StatCard",
    dir: "col",
    gap: 4,
    pad: 24,
    w: "fill",
    bg: "@surface",
    border: { color: "@border", width: 1 },
    radius: 12,
    children: [
      { type: "text", text: "@prop.value", style: "h1", w: "fill" },
      { type: "text", text: "@prop.label", style: "small", color: "@muted", w: "fill" }
    ]
  };
  return {
    name: "StatCard",
    props: [
      { name: "value", type: "text", default: "128k" },
      { name: "label", type: "text", default: "Monthly active" }
    ],
    body
  };
}
function pricingCardComponent() {
  const body = {
    type: "stack",
    name: "PricingCard",
    dir: "col",
    gap: 12,
    pad: 24,
    w: "fill",
    bg: "@surface",
    border: { color: "@border", width: 1 },
    shadow: "lg",
    radius: 16,
    children: [
      { type: "text", text: "@prop.plan", style: "h3" },
      { type: "text", text: "@prop.price", style: "display", w: "fill" },
      primaryPill("@prop.cta")
    ]
  };
  return {
    name: "PricingCard",
    props: [
      { name: "plan", type: "text", default: "Pro" },
      { name: "price", type: "text", default: "$29/mo" },
      { name: "cta", type: "text", default: "Choose Pro" }
    ],
    body
  };
}
function showcaseCell(name) {
  return {
    type: "stack",
    name: `cell-${name}`,
    dir: "col",
    gap: 10,
    grow: 1,
    children: [
      { type: "text", text: name, style: "label", color: "@muted" },
      { type: "instance", component: name, w: "fill" }
    ]
  };
}
function sheet(names, columns) {
  const rows = [];
  for (let i = 0; i < names.length; i += columns) {
    const cells = names.slice(i, i + columns).map(showcaseCell);
    for (let pad = cells.length; pad < columns; pad += 1) {
      cells.push({ type: "stack", name: "col-spacer", dir: "col", grow: 1, children: [] });
    }
    rows.push({ type: "stack", name: "row", dir: "row", gap: 24, align: "start", w: "fill", children: cells });
  }
  return { type: "stack", name: "sheet", dir: "col", gap: 24, w: "fill", children: rows };
}
function buildComponentKit() {
  const components = [
    buttonComponent(),
    featureCardComponent(),
    statCardComponent(),
    pricingCardComponent()
  ];
  const names = components.map((c) => c.name);
  const showcase = [
    {
      role: "content",
      children: [
        { type: "text", text: "The component library", style: "h2", w: "fill" },
        {
          type: "text",
          text: "Reusable masters synthesised from the brand \u2014 instantiate anywhere.",
          style: "body",
          color: "@muted",
          w: "fill"
        },
        sheet(names, 2)
      ]
    }
  ];
  return { components, showcase };
}

// src/studio/compose.ts
function navSection(name) {
  return {
    role: "nav",
    sticky: true,
    brand: { text: name },
    links: [
      { label: "Product", href: "#product" },
      { label: "Features", href: "#features" },
      { label: "Pricing", href: "#pricing" },
      { label: "About", href: "#about" }
    ],
    actions: [
      { type: "button", label: "Sign in", variant: "ghost" },
      { type: "button", label: "Get started", variant: "primary", icon: "lucide:arrow-right", iconPos: "right" }
    ]
  };
}
function footerSection(name) {
  return {
    role: "footer",
    bg: "@surface",
    brand: { text: name },
    columns: [
      { title: "Product", links: [{ label: "Features" }, { label: "Pricing" }, { label: "Dashboard" }] },
      { title: "Company", links: [{ label: "About" }, { label: "Contact" }] }
    ],
    legal: `\xA9 ${(/* @__PURE__ */ new Date()).getFullYear()} ${name}. Built with Plumb.`
  };
}
function bareBrief2(brief) {
  return brief.trim().replace(/^(a|an|the)\s+/i, "").replace(/[.!?]+$/, "");
}
function refImage(ctx, ref) {
  return { query: ctx.refImageQuery(ref), kind: "photo" };
}
function kitStrip(ctx) {
  if (ctx.kitComponentNames.length === 0) return void 0;
  const heading2 = {
    type: "text",
    text: "Built from the component library",
    style: "h2",
    w: "fill"
  };
  const row = {
    type: "stack",
    name: "kit-strip",
    dir: "row",
    gap: 24,
    wrap: true,
    align: "start",
    w: "fill",
    children: ctx.kitComponentNames.map((component) => ({
      type: "instance",
      component
    }))
  };
  return { role: "content", children: [heading2, row] };
}
function landing(ctx) {
  const { name, refs } = ctx;
  const bare = bareBrief2(ctx.brief);
  const heroRef = refs[0];
  const headline = bare ? bare.charAt(0).toUpperCase() + bare.slice(1) : name;
  const features2 = [
    {
      icon: "lucide:sparkles",
      title: "Crafted for the brief",
      body: `Every section is composed to match the tone of "${bare || ctx.brief}" \u2014 not filled into a generic template.`
    },
    {
      icon: "lucide:palette",
      title: "One coherent palette",
      body: `Colours are extracted live from ${refs.length} reference site(s) and synthesized into a single semantic system.`
    },
    {
      icon: "lucide:layout-grid",
      title: "Built end to end",
      body: "Nav, hero, features, gallery, and a call to action \u2014 a full page composed to the brief."
    }
  ];
  const galleryCards = Array.from({ length: 3 }, (_, i) => {
    const ref = refs[i % refs.length];
    return { image: refImage(ctx, ref), title: ref.name, body: ref.why };
  });
  return [
    navSection(name),
    {
      role: "hero",
      layout: "split-right",
      maxWidth: 1200,
      eyebrow: "INTRODUCING",
      headline,
      sub: `A complete, on-brand landing page for ${bare || ctx.brief} \u2014 composed from a single brief and ${refs.length} live reference site(s).`,
      actions: [
        { type: "button", label: "Get started", variant: "primary", size: "lg" },
        { type: "button", label: "See how it works", variant: "outline", size: "lg" }
      ],
      media: {
        type: "image",
        src: { query: ctx.refImageQuery(heroRef), kind: "photo", role: "hero" },
        w: "fill",
        h: 420,
        radius: "lg"
      }
    },
    {
      role: "features",
      heading: "Everything you need",
      bg: "@surface",
      layout: "row",
      features: features2
    },
    {
      role: "card-grid",
      heading: "Inspired by the best",
      columns: 3,
      cards: galleryCards
    },
    {
      role: "cta",
      bg: "@primary",
      theme: "dark",
      headline: `Start with ${name}`,
      sub: "Free to explore \u2014 composed and ready to ship.",
      actions: [{ type: "button", label: "Get started", variant: "secondary", size: "lg" }]
    },
    footerSection(name)
  ];
}
function features(ctx) {
  const { name } = ctx;
  const bare = bareBrief2(ctx.brief);
  const cards = [
    { icon: "lucide:sparkles", title: "Composed, not templated", body: "Each section is authored to the brief so the page reads as a considered whole." },
    { icon: "lucide:palette", title: "One coherent palette", body: "A single semantic colour system, synthesized from live reference sites." },
    { icon: "lucide:layout-grid", title: "Real sections", body: "Nav, hero, features, gallery, cta and footer \u2014 the same primitives across every page." },
    { icon: "lucide:gauge", title: "Fast to review", body: "Built page by page onto named Figma pages, so you can review between steps." },
    { icon: "lucide:component", title: "A real component library", body: "Reusable Figma component masters you can instantiate across the design." },
    { icon: "lucide:wand-2", title: "On brand throughout", body: `Type, spacing and colour stay consistent for "${bare || ctx.brief}".` }
  ];
  return [
    navSection(name),
    {
      role: "hero",
      layout: "center",
      maxWidth: 900,
      eyebrow: "FEATURES",
      headline: "Features",
      sub: `Everything ${name} composes for you, on brand and in sequence.`
    },
    {
      role: "features",
      heading: "Built for the whole product",
      bg: "@surface",
      layout: "row",
      features: cards
    },
    {
      role: "cta",
      bg: "@primary",
      theme: "dark",
      headline: `Design the rest with ${name}`,
      sub: "Every page composed to the same system.",
      actions: [{ type: "button", label: "Get started", variant: "secondary", size: "lg" }]
    },
    footerSection(name)
  ];
}
function pricing(ctx) {
  const { name } = ctx;
  const tiers = [
    {
      title: "Starter",
      body: "$0 / month \u2014 Everything you need to compose your first pages and review them in Figma.",
      action: { type: "button", label: "Start free", variant: "outline", size: "md" }
    },
    {
      title: "Pro",
      body: "$29 / month \u2014 A full component library, unlimited pages, and the director review loop.",
      action: { type: "button", label: "Go Pro", variant: "primary", size: "md" }
    },
    {
      title: "Team",
      body: "$99 / month \u2014 Shared brand foundations and every page composed to one system across the team.",
      action: { type: "button", label: "Contact sales", variant: "outline", size: "md" }
    }
  ];
  return [
    navSection(name),
    {
      role: "hero",
      layout: "center",
      maxWidth: 900,
      eyebrow: "PRICING",
      headline: "Simple, transparent pricing",
      sub: `Start free and scale as ${name} composes more of your product.`
    },
    {
      role: "card-grid",
      heading: "Choose your plan",
      columns: 3,
      cards: tiers
    },
    {
      role: "cta",
      bg: "@primary",
      theme: "dark",
      headline: "Ready when you are",
      sub: "No card required to explore.",
      actions: [{ type: "button", label: "Get started", variant: "secondary", size: "lg" }]
    },
    footerSection(name)
  ];
}
function dashboard(ctx) {
  const { name } = ctx;
  const stats = [
    { value: "12.4k", caption: "Active users" },
    { value: "98.2%", caption: "Uptime" },
    { value: "$1.2M", caption: "Processed" },
    { value: "4.9/5", caption: "Satisfaction" }
  ];
  const statTile = (stat) => ({
    type: "stack",
    name: "stat",
    dir: "col",
    gap: 8,
    pad: 24,
    grow: 1,
    w: "fill",
    bg: "@surface",
    border: { color: "@border", width: 1 },
    radius: 12,
    children: [
      { type: "text", text: stat.value, style: "h1" },
      { type: "text", text: stat.caption, style: "small", color: "@muted" }
    ]
  });
  const statRow = {
    type: "stack",
    name: "stats",
    dir: "row",
    gap: 24,
    w: "fill",
    children: stats.map(statTile)
  };
  const modules = [
    { icon: "lucide:bar-chart-3", title: "Analytics", body: "Live metrics across every surface, refreshed in real time." },
    { icon: "lucide:users", title: "Audience", body: "Segment and follow the people using your product." },
    { icon: "lucide:credit-card", title: "Billing", body: "Revenue, invoices and plans in one calm view." },
    { icon: "lucide:settings", title: "Settings", body: "Configure your workspace, team and integrations." },
    { icon: "lucide:bell", title: "Alerts", body: "Know the moment something needs your attention." },
    { icon: "lucide:shield", title: "Security", body: "Audit logs and access control, always on." }
  ];
  return [
    navSection(name),
    {
      role: "hero",
      layout: "center",
      maxWidth: 900,
      eyebrow: "DASHBOARD",
      headline: `${name} overview`,
      sub: "Your product at a glance."
    },
    { role: "content", bg: "@surface", children: [statRow] },
    {
      role: "card-grid",
      heading: "Modules",
      columns: 3,
      cards: modules
    },
    footerSection(name)
  ];
}
function composeSections(kind, ctx) {
  const build = {
    landing,
    features,
    pricing,
    dashboard
  };
  const sections = build[kind](ctx);
  const strip = kitStrip(ctx);
  if (strip) {
    const footerIdx = sections.length - 1;
    sections.splice(footerIdx, 0, strip);
  }
  return sections;
}

// src/studio/session.ts
var MAX_SESSIONS = 20;
var sessions = /* @__PURE__ */ new Map();
var counter = 0;
function evictOldest() {
  while (sessions.size > MAX_SESSIONS) {
    const oldest = sessions.keys().next().value;
    if (oldest === void 0) break;
    sessions.delete(oldest);
  }
}
function createSession(init) {
  const id = `studio-${++counter}`;
  const session = { ...init, id };
  sessions.set(id, session);
  evictOldest();
  return session;
}
function getSession(id) {
  return sessions.get(id);
}
function updateSession(id, patch) {
  const current = sessions.get(id);
  if (!current) return void 0;
  const next = { ...current, ...patch, id: current.id };
  sessions.set(id, next);
  return next;
}

// src/tools/studioFlow.ts
var STOPWORDS2 = /* @__PURE__ */ new Set(["a", "an", "the", "for", "with", "of", "and", "to", "that", "this", "your"]);
function deriveName2(brief) {
  const words = brief.split(/\s+/).filter(Boolean);
  const significant = words.filter((w) => !STOPWORDS2.has(w.toLowerCase()));
  const picked = (significant.length ? significant : words).slice(0, 3);
  const name = picked.map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
  return name || "Studio";
}
function toEmitTarget4(pageName) {
  return { kind: "page", pageName };
}
function studioBrandBlock(colors) {
  return {
    fonts: { heading: "Inter", body: "Inter" },
    colors,
    type: {
      display: { size: 64, weight: 600, line: 1.05, font: "heading" },
      h1: { size: 40, weight: 600, line: 1.1, font: "heading" },
      h2: { size: 28, weight: 600, line: 1.2, font: "heading" },
      h3: { size: 20, weight: 600, line: 1.3, font: "heading" },
      body: { size: 16, weight: 400, line: 1.6, font: "body" },
      small: { size: 13, weight: 400, line: 1.5, font: "body" },
      label: { size: 12, weight: 600, line: 1.2, font: "body", transform: "upper" }
    },
    spacing: { xs: 4, sm: 8, md: 16, lg: 24, xl: 48 },
    radius: { sm: 8, md: 12, lg: 16, full: "full" }
  };
}
var bareResolver = {
  async resolve(spec) {
    return { kind: spec.kind ?? "photo" };
  }
};
function resolverFromCaptures(captures) {
  const assets = {};
  const byUrl = /* @__PURE__ */ new Map();
  for (const cap of captures) {
    assets[cap.assetKey] = { ext: "png", kind: "image", w: cap.w, h: cap.h };
    byUrl.set(cap.url, cap);
  }
  const resolver = {
    async resolve(spec) {
      const hit = byUrl.get(spec.query);
      if (hit) return { assetId: hit.assetKey, kind: "photo", w: hit.w, h: hit.h, scaleMode: "fill" };
      return { kind: spec.kind ?? "photo" };
    }
  };
  return { resolver, assets };
}
function requirePaired(tool) {
  if (!bridge.paired) {
    throw new PlumbError(
      "No Figma plugin is paired.",
      `${tool} writes through the plugin path \u2014 run the Plumb plugin in Figma and click 'Pair with Plumb'.`
    );
  }
}
var START_DESCRIPTION = "STEP 1 of the transparent studio flow. Direct a brand from a one-line brief: discover best-in-class reference sites, screenshot them live, synthesize a coherent semantic palette, and build a References + Brand board (real Figma Variables + text styles) on a named page \u2014 then OPEN A SESSION so the next steps can reuse the brand + captures. Returns { sessionId, name, brand, references }. Follow with plumb_studio_kit(sessionId), then plumb_studio_page(sessionId, \u2026). Requires the Plumb plugin to be paired.";
function registerPlumbStudioStart(server) {
  server.registerTool(
    "plumb_studio_start",
    {
      title: "Plumb \xB7 studio start (references + brand + open session)",
      description: START_DESCRIPTION,
      inputSchema: {
        brief: z18.string().min(1).describe('One-line description, e.g. "a premium fintech dashboard".'),
        count: z18.number().int().positive().optional().describe("How many reference sites to study (default 4)."),
        references: z18.array(z18.string()).optional().describe("Explicit reference URLs to study first."),
        pageName: z18.string().optional().describe('Figma page for the brand board (default "Brand").')
      },
      annotations: { readOnlyHint: false, idempotentHint: false, openWorldHint: false }
    },
    async (args) => {
      try {
        const brief = typeof args.brief === "string" ? args.brief.trim() : "";
        if (!brief) {
          throw new PlumbError(
            "A brief is required.",
            'Pass `brief` \u2014 a one-line description like "a premium fintech dashboard".'
          );
        }
        const count = args.count && args.count > 0 ? Math.floor(args.count) : 4;
        const references = Array.isArray(args.references) ? args.references : void 0;
        const pageName = typeof args.pageName === "string" && args.pageName.trim() ? args.pageName.trim() : "Brand";
        const refs = discoverReferences({ description: brief, references }, count);
        if (refs.length === 0) {
          throw new PlumbError(
            "Could not resolve any references for the brief.",
            "Add explicit `references` URLs or broaden the brief wording, then retry."
          );
        }
        requirePaired("plumb_studio_start");
        emitStudio({ kind: "log", tool: "plumb_studio_start", screen: pageName, summary: `capturing ${refs.length} reference site(s)\u2026` });
        const { captures, misses } = await captureUrls(refs.map((r) => r.url), { maxHeight: 2200, settleMs: 1600 });
        for (const cap of captures) {
          emitStudio({ kind: "log", tool: "plumb_studio_start", screen: pageName, summary: `captured ${cap.url} (${cap.width}\xD7${cap.height})` });
        }
        const rawPalettes = captures.map((c) => c.palette).filter((p) => !!p);
        if (rawPalettes.length === 0) {
          throw new PlumbError(
            "No reference screenshot yielded a palette to synthesize a brand from.",
            "Check network access + that Chrome can reach the reference sites (PLUMB_CHROME), then retry."
          );
        }
        const brand = synthesizeBrandFromMany(rawPalettes);
        const { doc, resolver, assets, foundations } = buildBrandBoard({
          brief: { description: brief },
          references: refs,
          captures,
          brand
        });
        let foundationsWarnings = [];
        try {
          const fr = await requestApplyFoundations(foundations);
          foundationsWarnings = fr.warnings;
        } catch (e) {
          foundationsWarnings = [`foundations apply failed: ${e instanceof Error ? e.message : String(e)}`];
        }
        let compiled;
        try {
          compiled = await compile(doc, { assets: resolver });
        } catch (e) {
          throw new PlumbError(
            `Compiling the brand board failed: ${e instanceof Error ? e.message : String(e)}`,
            "This is usually a malformed section/block. Retry, or simplify the brief."
          );
        }
        const planId = `studio-start-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const plan = lowerToEmitPlan(compiled.doc, {
          planId,
          target: toEmitTarget4(pageName),
          mode: "create",
          prune: false,
          reveal: true,
          components: compiled.components,
          assets
        });
        const { result, error } = await requestApplyDesign(plan, (p) => {
          emitStudio({ kind: "log", tool: "plumb_studio_start", screen: pageName, summary: `${p.phase} ${p.done}/${p.total}${p.note ? ` \u2014 ${p.note}` : ""}` });
        });
        if (error || !result) {
          throw new PlumbError(
            `The plugin could not build the brand board: ${error ?? "no result returned"}`,
            "Check that the plugin is still paired, then retry."
          );
        }
        const sessionCaptures = captures.map((c) => ({
          url: c.url,
          assetKey: stageInboundAsset(c.png, "png"),
          w: c.width,
          h: c.height
        }));
        const session = createSession({
          brief,
          name: deriveName2(brief),
          brand,
          refs,
          captures: sessionCaptures
        });
        emitStudio({ kind: "screen", tool: "plumb_studio_start", screen: pageName, summary: `brand ready \u2014 session ${session.id}` });
        return ok({
          sessionId: session.id,
          name: session.name,
          brand,
          references: refs.map((r) => ({ name: r.name, url: r.url })),
          captured: captures.length,
          misses,
          foundationsWarnings,
          rootId: result.rootId,
          next: `Watch the "${pageName}" page. Then call plumb_studio_kit("${session.id}") to build the component library.`
        });
      } catch (e) {
        return fail(e);
      }
    }
  );
}
var KIT_DESCRIPTION = "STEP 2 of the transparent studio flow. Build the reusable COMPONENT LIBRARY for a session's brand \u2014 real Figma component masters (Button, FeatureCard, StatCard, PricingCard) \u2014 onto a named page and put them on show. The masters inherit the session palette + type scale. Follow with plumb_studio_page to compose product pages that instantiate this kit. Requires a sessionId from plumb_studio_start and the Plumb plugin paired.";
function registerPlumbStudioKit(server) {
  server.registerTool(
    "plumb_studio_kit",
    {
      title: "Plumb \xB7 studio kit (build the component library)",
      description: KIT_DESCRIPTION,
      inputSchema: {
        sessionId: z18.string().min(1).describe("Session id from plumb_studio_start."),
        pageName: z18.string().optional().describe('Figma page for the component library (default "Components").')
      },
      annotations: { readOnlyHint: false, idempotentHint: false, openWorldHint: false }
    },
    async (args) => {
      try {
        const sessionId = typeof args.sessionId === "string" ? args.sessionId.trim() : "";
        const session = sessionId ? getSession(sessionId) : void 0;
        if (!session) {
          throw new PlumbError(
            "Unknown or expired sessionId.",
            "Call plumb_studio_start first and pass the sessionId it returns."
          );
        }
        requirePaired("plumb_studio_kit");
        const pageName = typeof args.pageName === "string" && args.pageName.trim() ? args.pageName.trim() : "Components";
        const kit = buildComponentKit();
        const doc = {
          version: "1",
          meta: { name: `${session.name} \u2014 Components`, description: `Component library for ${session.brief}` },
          brand: studioBrandBlock(session.brand),
          components: kit.components,
          pages: [{ name: pageName, width: 1440, bg: "@bg", sections: kit.showcase }]
        };
        let compiled;
        try {
          compiled = await compile(doc, { assets: bareResolver });
        } catch (e) {
          throw new PlumbError(
            `Compiling the component library failed: ${e instanceof Error ? e.message : String(e)}`,
            "Retry; if it persists this is a kit authoring bug."
          );
        }
        const planId = `studio-kit-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const plan = lowerToEmitPlan(compiled.doc, {
          planId,
          target: toEmitTarget4(pageName),
          mode: "create",
          prune: false,
          reveal: true,
          components: compiled.components
        });
        const { result, error } = await requestApplyDesign(plan, (p) => {
          emitStudio({ kind: "log", tool: "plumb_studio_kit", screen: pageName, summary: `${p.phase} ${p.done}/${p.total}${p.note ? ` \u2014 ${p.note}` : ""}` });
        });
        if (error || !result) {
          throw new PlumbError(
            `The plugin could not build the component library: ${error ?? "no result returned"}`,
            "Check that the plugin is still paired, then retry."
          );
        }
        const componentNames = kit.components.map((c) => c.name);
        updateSession(session.id, { kit: { components: kit.components, componentNames } });
        emitStudio({ kind: "screen", tool: "plumb_studio_kit", screen: pageName, summary: `component library built \u2014 ${componentNames.length} component(s)` });
        return ok({
          sessionId: session.id,
          components: componentNames,
          rootId: result.rootId,
          next: `Watch the "${pageName}" page. Then call plumb_studio_page("${session.id}", { pageName, kind }) to compose a product page (landing / features / pricing / dashboard).`
        });
      } catch (e) {
        return fail(e);
      }
    }
  );
}
var PAGE_DESCRIPTION = "STEP 3 of the transparent studio flow (repeatable). Compose a full PRODUCT PAGE of a given kind \u2014 landing / features / pricing / dashboard \u2014 onto its own named Figma page, reusing the session's brand + reference imagery and instantiating the component library. Returns { rootId, ids, authoredPath } \u2014 screenshot the rootId, grade it as director, then feed it to plumb_review for the critique loop. Call once per page to build a whole product page by page. Requires a sessionId from plumb_studio_start and the Plumb plugin paired.";
var PAGE_KINDS = ["landing", "features", "pricing", "dashboard"];
function registerPlumbStudioPage(server) {
  server.registerTool(
    "plumb_studio_page",
    {
      title: "Plumb \xB7 studio page (compose a product page)",
      description: PAGE_DESCRIPTION,
      inputSchema: {
        sessionId: z18.string().min(1).describe("Session id from plumb_studio_start."),
        pageName: z18.string().min(1).describe('Figma page to build onto, e.g. "Landing".'),
        kind: z18.enum(PAGE_KINDS).optional().describe("Which page to compose (default landing).")
      },
      annotations: { readOnlyHint: false, idempotentHint: false, openWorldHint: false }
    },
    async (args) => {
      try {
        const sessionId = typeof args.sessionId === "string" ? args.sessionId.trim() : "";
        const session = sessionId ? getSession(sessionId) : void 0;
        if (!session) {
          throw new PlumbError(
            "Unknown or expired sessionId.",
            "Call plumb_studio_start first and pass the sessionId it returns."
          );
        }
        requirePaired("plumb_studio_page");
        const pageName = typeof args.pageName === "string" && args.pageName.trim() ? args.pageName.trim() : "";
        if (!pageName) {
          throw new PlumbError("A pageName is required.", 'Pass `pageName` \u2014 the Figma page to build onto, e.g. "Landing".');
        }
        const kind = PAGE_KINDS.includes(args.kind) ? args.kind : "landing";
        const { resolver, assets } = resolverFromCaptures(session.captures);
        const sections = composeSections(kind, {
          brief: session.brief,
          name: session.name,
          refs: session.refs,
          refImageQuery: (ref) => ref.url,
          kitComponentNames: session.kit?.componentNames ?? []
        });
        const doc = {
          version: "1",
          meta: { name: `${session.name} \u2014 ${kind}`, description: session.brief },
          brand: studioBrandBlock(session.brand),
          components: session.kit?.components ?? [],
          pages: [{ name: pageName, width: 1440, bg: "@bg", sections }]
        };
        emitStudio({ kind: "log", tool: "plumb_studio_page", screen: pageName, summary: `composing ${kind} page\u2026` });
        let compiled;
        try {
          compiled = await compile(doc, { assets: resolver });
        } catch (e) {
          throw new PlumbError(
            `Compiling the ${kind} page failed: ${e instanceof Error ? e.message : String(e)}`,
            "This is usually a malformed section/block. Retry, or try another kind."
          );
        }
        const planId = `studio-page-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
        const plan = lowerToEmitPlan(compiled.doc, {
          planId,
          target: toEmitTarget4(pageName),
          mode: "create",
          prune: false,
          reveal: true,
          components: compiled.components,
          assets
        });
        const { result, error } = await requestApplyDesign(plan, (p) => {
          emitStudio({ kind: "log", tool: "plumb_studio_page", screen: pageName, summary: `${p.phase} ${p.done}/${p.total}${p.note ? ` \u2014 ${p.note}` : ""}` });
        });
        if (error || !result) {
          throw new PlumbError(
            `The plugin could not build the ${kind} page: ${error ?? "no result returned"}`,
            "Check that the plugin is still paired, then retry."
          );
        }
        const summary = `built ${kind} page \u2014 ${result.created} node(s)`;
        emitStudio({ kind: "screen", tool: "plumb_studio_page", screen: pageName, summary });
        let authoredPath;
        try {
          authoredPath = join13(tmpdir5(), `plumb-authored-${planId}.json`);
          writeFileSync7(authoredPath, JSON.stringify(compiled.doc));
        } catch {
          authoredPath = void 0;
        }
        return ok({
          sessionId: session.id,
          kind,
          rootId: result.rootId,
          ids: result.ids,
          authoredPath,
          warnings: compiled.warnings,
          summary,
          next: "Screenshot rootId, grade it as director, then plumb_review with { id: rootId, ids, authoredPath, director }."
        });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/tokens.ts
import { z as z19 } from "zod";
var DESCRIPTION17 = "Extract the deduplicated design-token table for a Figma node \u2014 colours, type styles, radii, shadows \u2014 as the $-prefixed refs the PDS node tree uses. With the Plumb plugin paired, pass a screen `id` or `name` (no file key). On the REST path, pass `fileKey` + `id`. Build with these tokens, not magic numbers.";
function registerPlumbTokens(server) {
  server.registerTool(
    "plumb_tokens",
    {
      title: "Plumb \xB7 tokens",
      description: DESCRIPTION17,
      inputSchema: {
        fileKey: z19.string().optional().describe("Figma file key \u2014 REST path. Omit when the Plumb plugin is paired."),
        url: z19.string().optional().describe(
          "Paste a full Figma URL \u2014 fileKey and node-id are auto-extracted."
        ),
        id: z19.string().optional().describe('Node id whose design tokens to extract, e.g. "131:9592".'),
        name: z19.string().optional().describe("Screen name \u2014 plugin path; resolved against the paired file.")
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: true }
    },
    async (args) => {
      try {
        const { fileKey, id } = resolveFigmaTarget({
          url: args.url,
          fileKey: args.fileKey,
          id: args.id
        });
        if (bridge.paired && (id || args.name)) {
          const resolved = resolveScreen(id, args.name);
          if ("ambiguous" in resolved) {
            return ok({
              ambiguous: true,
              matches: formatScreenMatches(resolved.ambiguous),
              next: "Several screens share that name. Each row shows `page` and `box` (w\xD7h) \u2014 pick the one you want and re-call plumb_tokens with its `id`."
            });
          }
          const { doc } = await requestNode(resolved.id);
          if (!doc) {
            throw new PlumbError(
              `The Plumb plugin could not find node "${resolved.id}".`,
              "Call plumb_outline for the current screen list \u2014 it may have been deleted or renamed."
            );
          }
          const file2 = {
            document: doc,
            fileName: bridge.inventory?.fileName ?? "",
            version: `plugin-${Date.now()}`
          };
          const pds2 = normalize2(file2, 99, {});
          const result2 = {
            tokens: pds2.tokens,
            meta: {
              fromNode: resolved.id,
              nodeCount: pds2.meta.nodeCount,
              estTokens: estimateTokens(JSON.stringify(pds2.tokens))
            },
            note: "Tokens are deduplicated across the entire subtree. Variable bindings (modes, aliases) surface on the PDS node fills/strokes.",
            next: "Use these $-refs while building; call plumb_node for the node tree."
          };
          return ok({ ...result2, source: "plugin" });
        }
        if (!fileKey || !id) {
          throw new PlumbError(
            "plumb_tokens needs the Plumb plugin paired, or a fileKey + id (or a Figma url) for the REST path.",
            "Pair the Plumb plugin in Figma, paste a Figma URL via `url`, or pass both fileKey and id."
          );
        }
        const cacheKey = `tokens:${fileKey}:${id}`;
        const hit = cacheGet(cacheKey, DEFAULT_TTL_MS);
        if (hit) return ok({ ...hit.payload, source: "rest", cached: true });
        const token = requireToken2();
        const file = await fetchNodeViaRest({
          fileKey,
          nodeId: id,
          depth: 24,
          token
        });
        const pds = normalize2(file, 99, {});
        const result = {
          tokens: pds.tokens,
          meta: {
            fromNode: id,
            nodeCount: pds.meta.nodeCount,
            estTokens: estimateTokens(JSON.stringify(pds.tokens))
          },
          note: "Tokens are deduplicated from node styles via the REST path. Figma Variables (modes, aliases, scopes) require the plugin path.",
          next: "Use these $-refs while building; call plumb_node for the node tree."
        };
        cacheSet(cacheKey, file.version, result);
        return ok({ ...result, source: "rest", cached: false });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/tools/verify.ts
import { z as z20 } from "zod";
var DESCRIPTION18 = "Compare what you built against the Figma design and return structured deltas \u2014 exact, deterministic, no pixel diff. After rendering, for every element you tagged `data-plumb-id=\"<el>\"` (or the globally-unique `data-plumb-id=\"<path>\"` for deeply nested DOM), collect:\n  \u2022 box \u2014 getBoundingClientRect() \u2192 { x, y, w, h }\n  \u2022 styles \u2014 a subset of getComputedStyle: backgroundColor, color, fontFamily, fontSize, fontWeight, lineHeight, padding{Top,Right,Bottom,Left}, gap, flexDirection, justifyContent, alignItems, borderRadius, borderColor, borderWidth, opacity, textDecorationLine, boxShadow, backdropFilter\n  \u2022 text \u2014 textContent for TEXT nodes\n  \u2022 asset \u2014 for image/icon/logo nodes (assetId or vector): the data-plumb-asset=\"<assetId>\" you rendered, plus img:true when it is a real <img>/<svg> (not a redrawn div). Verify errors on a visual node rendered with no real asset, so a redrawn/omitted logo lowers the score.\nPass them as `rendered`. The tool joins by `el` (loose) or `path` (strict) and returns deltas like { kind:'size.w', expected:528, actual:530, severity:'warn' }. The response also includes `coverage` \u2014 how many PDS els in the subtree were actually tagged, plus an `untagged` list so you know what to add next round. QA-strict on appearance \u2014 colour (\u0394E2000), icon/asset fidelity, box-shadow, backdrop-filter (glass) and the rest flag even small misses \u2014 but content-aware on text: a mismatch on placeholder/template copy (lorem, generic labels, numeric stubs, copy-pasted cells) is advisory `info` (kind 'text.placeholder', doesn't dent the score), since you're meant to swap real content in. Real UI labels still warn (kind 'text.chars'). ok=true means no errors; warns are differences you may have meant.";
var renderedElementSchema2 = z20.object({
  el: z20.string(),
  box: z20.object({
    x: z20.number(),
    y: z20.number(),
    w: z20.number(),
    h: z20.number()
  }),
  text: z20.string().optional(),
  styles: z20.record(z20.string(), z20.string()).optional(),
  // Asset-fidelity inputs — without these, zod strips them and every tagged
  // image/icon/logo falls into the "asset.missing" branch (false errors).
  asset: z20.string().optional(),
  img: z20.boolean().optional()
});
function registerPlumbVerify(server) {
  server.registerTool(
    "plumb_verify",
    {
      title: "Plumb \xB7 verify",
      description: DESCRIPTION18,
      inputSchema: {
        id: z20.string().optional().describe("Screen id."),
        name: z20.string().optional().describe("Screen name (plugin path)."),
        fileKey: z20.string().optional().describe("File key (REST path)."),
        url: z20.string().optional().describe(
          "Paste a full Figma URL \u2014 fileKey and node-id are auto-extracted."
        ),
        rendered: z20.array(renderedElementSchema2).describe('Each element you tagged data-plumb-id="<el>".'),
        viewport: z20.object({ w: z20.number(), h: z20.number() }).optional().describe("Browser viewport size (informational)."),
        tolerances: z20.object({
          px: z20.object({ ok: z20.number(), warn: z20.number() }).optional(),
          color: z20.object({ ok: z20.number(), warn: z20.number() }).optional()
        }).optional().describe("Override default thresholds."),
        depth: z20.number().int().positive().optional().describe("PDS depth to fetch. Default 12 \u2014 deep enough for most screens.")
      },
      annotations: { readOnlyHint: true, idempotentHint: true, openWorldHint: false }
    },
    async (args) => {
      try {
        if (!args.rendered || args.rendered.length === 0) {
          throw new PlumbError(
            "plumb_verify needs the rendered layout.",
            "For every element you tagged data-plumb-id, send { el, box: getBoundingClientRect(), styles: getComputedStyle subset }. See the tool description."
          );
        }
        const depth = args.depth ?? 12;
        const target = await resolveVerifyTarget(args, depth);
        if (target.kind === "ambiguous") {
          return ok({
            ambiguous: true,
            matches: target.matches,
            next: "Several screens share that name. Each row shows `page` and `box` (w\xD7h) \u2014 pick the one you want and re-call plumb_verify with its `id`."
          });
        }
        const { source, screen, pds } = target;
        const tolerances = {
          px: { ...DEFAULT_TOLERANCES.px, ...args.tolerances?.px ?? {} },
          color: { ...DEFAULT_TOLERANCES.color, ...args.tolerances?.color ?? {} }
        };
        const result = verifyAgainst(pds, args.rendered, tolerances);
        const errs = result.deltas.filter((d) => d.severity === "error").length;
        const warns = result.deltas.filter((d) => d.severity === "warn").length;
        emitStudio({
          kind: "tool",
          tool: "plumb_verify",
          screen,
          deltas: result.deltas.slice(0, 12).map((d) => ({ el: d.el, kind: d.kind, severity: d.severity })),
          summary: `verify \u2014 ${result.matched} matched, ${errs} err / ${warns} warn`
        });
        const cov = result.coverage;
        const coveragePct = cov ? Math.round(cov.coverage * 100) : 100;
        const lowCoverage = cov && cov.coverage < 0.6 && cov.untagged.length > 0;
        const taggingHint = cov && cov.untagged.length > 0 ? ` Consider tagging more: ${cov.untagged.slice(0, 8).join(", ")}${cov.untagged.length > 8 ? ", \u2026" : ""}.` : "";
        let next;
        if (result.ok && !lowCoverage) {
          next = `Pixel-perfect within tolerances \u2014 ${result.matched}/${cov?.pdsTotal ?? result.matched} matched (${coveragePct}% coverage), ${result.deltas.length} note(s).`;
        } else if (result.ok && lowCoverage) {
          next = `No deltas, but only ${coveragePct}% of the PDS subtree was tagged. "All matched" with ${coveragePct}% coverage is not the same as "the page is right".${taggingHint}`;
        } else {
          next = `Fix the ${errs} error(s)${warns ? ` and consider the ${warns} warning(s)` : ""}, then call plumb_verify again. Coverage: ${coveragePct}%.${result.unmatched > 0 ? ` (${result.unmatched} rendered element(s) had no PDS match \u2014 check your data-plumb-id tags or increase \`depth\`.)` : ""}${taggingHint}`;
        }
        return ok({ source, screen, ...result, next });
      } catch (e) {
        return fail(e);
      }
    }
  );
}

// src/server.ts
function createServer3() {
  const server = new McpServer(
    { name: SERVER_NAME, version: SERVER_VERSION },
    {
      instructions: "Plumb extracts Figma designs as compact, normalized specs for building UI, then verifies what you built. Use Plumb when Figma's official Dev Mode MCP hits its plan limit (6 tool calls/month on Starter), exceeds the 25k token cap (351k tokens observed on real screens), or when Framelink (figma-developer-mcp) returns Figma REST 429 \u2014 Plumb's plugin path bypasses both and works on every Figma plan including Free, with no metered billing and no per-call quotas. Call plumb_status first. With the plugin paired: plumb_outline lists every screen; plumb_node extracts one by id or name; plumb_selection extracts the live selection; plumb_assets exports icons/images (list / surgical modes); plumb_screenshot renders a node to PNG; plumb_search finds nodes; plumb_components lists components and instances; plumb_verify diffs your rendered layout against the design and returns structured deltas; plumb_fit wraps that diff in a 0\u2013100 convergence score + prioritised fixes so you can iterate to a pixel-perfect match (build \u2192 plumb_fit \u2192 fix \u2192 repeat until done); plumb_query pulls a slice by pattern (skeleton / buttons / text / components) when the full tree would be too big. Otherwise use the REST path (fileKey + id). Auto-layout is pre-resolved to flexbox."
    }
  );
  registerPlumbStatus(server);
  registerPlumbOutline(server);
  registerPlumbNode(server);
  registerPlumbTokens(server);
  registerPlumbSelection(server);
  registerPlumbAssets(server);
  registerPlumbScreenshot(server);
  registerPlumbDescribe(server);
  registerPlumbSearch(server);
  registerPlumbComponents(server);
  registerPlumbVerify(server);
  registerPlumbFit(server);
  registerPlumbQuery(server);
  registerPlumbFigOutline(server);
  registerPlumbFigNode(server);
  registerPlumbDesign(server);
  registerBrandTool(server);
  registerPlumbStudio(server);
  registerPlumbSource(server);
  registerPlumbReview(server);
  registerPlumbStudioStart(server);
  registerPlumbStudioKit(server);
  registerPlumbStudioPage(server);
  return server;
}

// src/index.ts
var HELP4 = `plumb-mcp ${SERVER_VERSION} \u2014 Figma MCP server for AI coding agents

Usage:
  plumb-mcp                  Run the stdio MCP server (+ the local plugin bridge).
                             This is what your MCP client (Claude Code, Cursor,
                             Windsurf, etc.) spawns.

  plumb-mcp init             Detect your installed editor(s) and write the
                             correct MCP config block into each \u2014 Claude Code,
                             Cursor, VS Code, Windsurf. Existing entries are
                             preserved.

  plumb-mcp verify <url>     Drive headless Chrome against your dev server,
                             capture every [data-plumb-id] element's box +
                             computed styles, and print the deltas against the
                             Figma design. No Puppeteer needed \u2014 uses your
                             installed Chrome. Run \`plumb-mcp verify --help\`
                             for the full option list.

  plumb-mcp fit <figma-url>  Self-healing build loop: generate an HTML build
                             from the design, render it, diff it, and correct
                             it pass-by-pass until it matches pixel-for-pixel.
                             Needs ANTHROPIC_API_KEY. Run \`plumb-mcp fit --help\`.

  plumb-mcp studio           Open Plumb Studio \u2014 a live local cockpit that
                             mirrors what your AI agent is doing (design, live
                             build, match score, activity) and can drive Plumb's
                             own self-healing loop with in-UI approvals.

  plumb-mcp --help, -h       Print this message and exit.
  plumb-mcp --version, -v    Print the version and exit.

Fifteen tools exposed once running: plumb_status, plumb_outline, plumb_node,
plumb_tokens, plumb_selection, plumb_assets, plumb_screenshot, plumb_describe,
plumb_search, plumb_components, plumb_verify, plumb_fit, plumb_query,
plumb_fig_outline, plumb_fig_node.

Docs:    https://tathagat22.github.io/plumb-mcp/
Source:  https://github.com/tathagat22/plumb-mcp
`;
async function main() {
  loadEnv();
  const arg = process.argv[2];
  if (arg === "--help" || arg === "-h" || arg === "help") {
    process.stdout.write(HELP4);
    return;
  }
  if (arg === "--version" || arg === "-v") {
    process.stdout.write(`${SERVER_VERSION}
`);
    return;
  }
  if (arg === "init") {
    runInit();
    return;
  }
  if (arg === "verify") {
    const code = await runVerifyCli(process.argv.slice(3));
    process.exit(code);
  }
  if (arg === "fit") {
    const code = await runFitCli(process.argv.slice(3));
    process.exit(code);
  }
  if (arg === "studio") {
    const code = await runStudioCli(process.argv.slice(3));
    process.exit(code);
  }
  if (arg && arg.startsWith("-")) {
    process.stderr.write(
      `plumb-mcp: unknown flag "${arg}". Run \`plumb-mcp --help\` for usage.
`
    );
    process.exit(2);
  }
  const server = createServer3();
  await startBridge();
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write(`plumb-mcp ${SERVER_VERSION} running (stdio)
`);
}
main().catch((e) => {
  process.stderr.write(
    `plumb-mcp failed to start: ${e instanceof Error ? e.message : String(e)}
`
  );
  process.exit(1);
});
