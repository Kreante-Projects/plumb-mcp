// Regression tests for the Kreante security patch of the Figma plugin.
// Run with: node --test tests/
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

const read = (file) => readFileSync(new URL(`../figma-plugin/${file}`, import.meta.url), "utf8");
const flush = async () => {
  for (let i = 0; i < 5; i++) await new Promise((resolve) => setImmediate(resolve));
};
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function element() {
  return {
    classList: { add() {}, remove() {} },
    style: {},
    firstChild: null,
    appendChild() {},
    removeChild() {},
    setAttribute() {},
    innerHTML: "",
    textContent: "",
  };
}

function loadUi(fetchImpl = async () => ({ ok: true })) {
  const source = read("ui.html").match(/<script>([\s\S]*)<\/script>/)[1];
  const sockets = [];
  const posted = [];
  const fetches = [];

  class FakeWebSocket {
    constructor(url) {
      this.url = url;
      this.readyState = 1;
      this.sent = [];
      sockets.push(this);
    }
    send(raw) {
      this.sent.push(JSON.parse(raw));
    }
    close() {
      this.readyState = 3;
      if (this.onclose) this.onclose();
    }
  }

  const context = {
    document: { getElementById: element, createElement: element, body: element() },
    parent: { postMessage: (message) => posted.push(message.pluginMessage) },
    WebSocket: FakeWebSocket,
    setTimeout: () => 0,
    clearTimeout() {},
    fetch: (url, options) => {
      fetches.push(url);
      return fetchImpl(url, options);
    },
  };
  context.window = context;
  vm.createContext(context);
  vm.runInContext(source, context);

  const ws = sockets.find((socket) => socket.url === "ws://localhost:31337");
  return {
    ws,
    posted,
    fetches,
    fromServer: (message) => ws.onmessage({ data: JSON.stringify(message) }),
    fromWorker: (message) => context.window.onmessage({ data: { pluginMessage: message } }),
    requests: () => posted.filter((message) => message.type === "server-request"),
    acks: () => posted.filter((message) => message.type === "upload-ack"),
  };
}

function pair(ui) {
  ui.fromServer({ t: "plumb-hello", sessionLabel: "test" });
  ui.fromServer({ t: "paired" });
}

function extractFunction(source, name) {
  const match = new RegExp(`(?:async )?function ${name}\\(`).exec(source);
  if (!match) return "";
  let index = source.indexOf("{", source.indexOf(")", match.index));
  let depth = 0;
  for (let cursor = index; cursor < source.length; cursor++) {
    if (source[cursor] === "{") depth++;
    if (source[cursor] === "}" && --depth === 0) return source.slice(match.index, cursor + 1);
  }
  throw new Error(`Unbalanced function: ${name}`);
}

function loadBuildActions() {
  const source = read("code.js");
  const context = {};
  vm.createContext(context);
  vm.runInContext(
    `var NODE_NAVS = [];
     ${extractFunction(source, "isSafeLinkUrl")}
     ${extractFunction(source, "buildActions")}
     this.buildActions = buildActions;`,
    context,
  );
  return context.buildActions;
}

function loadUploadBytes() {
  const source = read("code.js");
  const context = {
    figma: { ui: { postMessage() {} } },
    setTimeout,
    clearTimeout,
  };
  vm.createContext(context);
  vm.runInContext(
    `var uploadAcks = new Map();
     var UPLOAD_ACK_TIMEOUT_MS = 5;
     ${extractFunction(source, "uploadBytes")}
     this.uploadBytes = uploadBytes;
     this.uploadAcks = uploadAcks;`,
    context,
  );
  return context;
}

const outcomeOf = (promise) =>
  Promise.race([
    promise.then(
      () => "resolved",
      (error) => `rejected: ${error.message}`,
    ),
    sleep(300).then(() => "hung"),
  ]);

test("an unpaired server cannot send requests to the plugin worker", () => {
  const ui = loadUi();
  ui.fromServer({ t: "plumb-hello", sessionLabel: "test" });
  ui.fromServer({ t: "get-node", reqId: "r1" });
  ui.fromServer({ t: "apply-design", reqId: "r2", plan: {} });
  assert.equal(ui.requests().length, 0);
});

test("a paired server can still send requests to the plugin worker", () => {
  const ui = loadUi();
  pair(ui);
  ui.fromServer({ t: "get-node", reqId: "r1" });
  assert.equal(ui.requests().length, 1);
  assert.equal(ui.requests()[0].req.reqId, "p31337_r1");
});

test("a design is dropped if the session closes while its assets hydrate", async () => {
  let finishFetch;
  const ui = loadUi(() => new Promise((resolve) => (finishFetch = resolve)));
  pair(ui);
  ui.fromServer({
    t: "apply-design",
    reqId: "r1",
    plan: { assets: [{ ref: "a", ext: "png" }] },
  });
  ui.ws.close();
  finishFetch({ ok: true, arrayBuffer: async () => new ArrayBuffer(1) });
  await flush();
  assert.equal(ui.requests().length, 0);
});

test("an asset upload without a live session is acknowledged with an error", async () => {
  const ui = loadUi();
  ui.fromWorker({ type: "upload-asset", reqId: "p31337_gone", index: 0, bytes: [], ext: "png" });
  await flush();
  assert.equal(ui.fetches.length, 0);
  assert.equal(ui.acks().length, 1);
  assert.equal(typeof ui.acks()[0].error, "string");
});

test("an asset upload on a paired session still succeeds", async () => {
  const ui = loadUi();
  pair(ui);
  ui.fromServer({ t: "get-assets", reqId: "r1" });
  ui.fromWorker({ type: "upload-asset", reqId: "p31337_r1", index: 0, bytes: [], ext: "png" });
  await flush();
  assert.equal(ui.fetches.length, 1);
  assert.equal(ui.acks()[0].error, null);
});

test("a screenshot without a live session is never uploaded", async () => {
  const ui = loadUi();
  ui.fromWorker({ type: "upload-screenshot", reqId: "p31337_gone", bytes: [], format: "PNG" });
  await flush();
  assert.equal(ui.fetches.length, 0);
});

test("a missing upload acknowledgement times out instead of hanging", async () => {
  const worker = loadUploadBytes();
  const outcome = await outcomeOf(worker.uploadBytes([], "png", "r1", 0));
  assert.match(outcome, /^rejected: .*timed out/i);
  assert.equal(worker.uploadAcks.size, 0);
});

test("upload acknowledgements still resolve and report errors", async () => {
  const worker = loadUploadBytes();
  const success = worker.uploadBytes([], "png", "r1", 0);
  worker.uploadAcks.get("r1-0")(null);
  assert.equal(await outcomeOf(success), "resolved");

  const failure = worker.uploadBytes([], "png", "r1", 1);
  worker.uploadAcks.get("r1-1")("HTTP 500");
  assert.equal(await outcomeOf(failure), "rejected: HTTP 500");
});

test("prototype links accept only http and https URLs", () => {
  const buildActions = loadBuildActions();
  const actionsFor = (url) => buildActions({ navigation: "URL", url }).length;

  for (const url of ["https://example.com/a?b=1", "http://example.com", "HTTPS://EXAMPLE.COM"]) {
    assert.equal(actionsFor(url), 1, url);
  }
  for (const url of [
    "javascript:alert(1)",
    "data:text/html,hello",
    "file:///etc/passwd",
    "https://exa mple.com",
    " https://example.com",
    "https://example.com\n.evil",
    "https://",
    "",
    undefined,
    42,
  ]) {
    assert.equal(actionsFor(url), 0, String(url));
  }
});
