"use strict";
(() => {
  var __defProp = Object.defineProperty;
  var __getOwnPropSymbols = Object.getOwnPropertySymbols;
  var __hasOwnProp = Object.prototype.hasOwnProperty;
  var __propIsEnum = Object.prototype.propertyIsEnumerable;
  var __defNormalProp = (obj, key, value) => key in obj ? __defProp(obj, key, { enumerable: true, configurable: true, writable: true, value }) : obj[key] = value;
  var __spreadValues = (a, b) => {
    for (var prop in b || (b = {}))
      if (__hasOwnProp.call(b, prop))
        __defNormalProp(a, prop, b[prop]);
    if (__getOwnPropSymbols)
      for (var prop of __getOwnPropSymbols(b)) {
        if (__propIsEnum.call(b, prop))
          __defNormalProp(a, prop, b[prop]);
      }
    return a;
  };
  var __objRest = (source, exclude) => {
    var target = {};
    for (var prop in source)
      if (__hasOwnProp.call(source, prop) && exclude.indexOf(prop) < 0)
        target[prop] = source[prop];
    if (source != null && __getOwnPropSymbols)
      for (var prop of __getOwnPropSymbols(source)) {
        if (exclude.indexOf(prop) < 0 && __propIsEnum.call(source, prop))
          target[prop] = source[prop];
      }
    return target;
  };

  // figma-plugin/emit.ts
  var PLUMB_KEY = "plumbKey";
  var FALLBACK_FONTS = [
    { family: "Inter", style: "Regular" },
    { family: "Roboto", style: "Regular" }
  ];
  function faceKey(f) {
    return `${f.family}\0${f.style}`;
  }
  function errText(e) {
    return e instanceof Error ? e.message : String(e);
  }
  function tryset(warnings, key, field, fn) {
    try {
      fn();
    } catch (e) {
      warnings.push({ key, field, message: errText(e) });
    }
  }
  async function loadFonts(faces, warnings, onProgress) {
    const loaded = /* @__PURE__ */ new Set();
    async function tryLoad(f) {
      const k = faceKey(f);
      if (loaded.has(k)) return true;
      try {
        await figma.loadFontAsync({ family: f.family, style: f.style });
        loaded.add(k);
        return true;
      } catch (e) {
        return false;
      }
    }
    let fallback = null;
    for (const fb of FALLBACK_FONTS) {
      if (await tryLoad(fb)) {
        fallback = fb;
        break;
      }
    }
    const requested = faces && faces.length ? faces : [];
    const total = requested.length;
    const remap = /* @__PURE__ */ new Map();
    let done = 0;
    for (const f of requested) {
      const ok = await tryLoad(f);
      if (!ok) {
        const use = fallback != null ? fallback : f;
        remap.set(faceKey(f), use);
        warnings.push({
          key: "@font",
          field: "font",
          message: `font "${f.family} ${f.style}" unavailable \u2192 fell back to "${use.family} ${use.style}"`
        });
      }
      done += 1;
      onProgress == null ? void 0 : onProgress("variables", done, total, "fonts");
    }
    return (f) => {
      const direct = remap.get(faceKey(f));
      if (direct) return direct;
      if (loaded.has(faceKey(f))) return f;
      return fallback != null ? fallback : f;
    };
  }
  async function ensureFace(f, resolve, warnings) {
    try {
      await figma.loadFontAsync({ family: f.family, style: f.style });
      return f;
    } catch (e) {
      const use = resolve(f);
      warnings.push({
        key: "@font",
        field: "run.font",
        message: `run font "${f.family} ${f.style}" unavailable \u2192 "${use.family} ${use.style}"`
      });
      return use;
    }
  }
  function decodeUtf8(bytes) {
    if (typeof TextDecoder !== "undefined") return new TextDecoder("utf-8").decode(bytes);
    let s = "";
    for (let i = 0; i < bytes.length; i += 1) s += String.fromCharCode(bytes[i]);
    return decodeURIComponent(escape(s));
  }
  function hydrateAssets(assets, warnings, onProgress) {
    var _a;
    const images = /* @__PURE__ */ new Map();
    const svgs = /* @__PURE__ */ new Map();
    const list = assets != null ? assets : [];
    const total = list.length;
    let done = 0;
    for (const a of list) {
      try {
        if (a.kind === "svg") {
          const text = (_a = a.svgInline) != null ? _a : a.data ? decodeUtf8(a.data) : "";
          if (text) svgs.set(a.ref, text);
          else warnings.push({ key: a.ref, field: "asset", message: "svg asset had no data" });
        } else {
          if (a.data && a.data.length) {
            const img = figma.createImage(a.data);
            images.set(a.ref, img.hash);
          } else {
            warnings.push({ key: a.ref, field: "asset", message: "image asset had no bytes" });
          }
        }
      } catch (e) {
        warnings.push({ key: a.ref, field: "asset", message: errText(e) });
      }
      done += 1;
      onProgress == null ? void 0 : onProgress("assets", done, total);
    }
    return { images, svgs };
  }
  function toPaint(p, assets, warnings, key) {
    var _a, _b;
    if (p.type === "SOLID") {
      return { type: "SOLID", color: p.color, opacity: p.opacity };
    }
    if (p.type === "IMAGE") {
      const hash = assets.images.get(p.assetRef);
      if (!hash) {
        if (!assets.svgs.has(p.assetRef)) {
          warnings.push({ key, field: "fills", message: `image ref "${p.assetRef}" not hydrated` });
        }
        return null;
      }
      return {
        type: "IMAGE",
        imageHash: hash,
        scaleMode: (_a = p.scaleMode) != null ? _a : "FILL",
        opacity: p.opacity
      };
    }
    const transform = (_b = p.transform) != null ? _b : [
      [1, 0, 0],
      [0, 1, 0]
    ];
    return {
      type: p.type,
      gradientTransform: transform,
      gradientStops: p.stops.map((s) => ({ position: s.position, color: s.color })),
      opacity: p.opacity
    };
  }
  function toPaints(ps, assets, warnings, key) {
    if (!ps) return [];
    const out = [];
    for (const p of ps) {
      const paint = toPaint(p, assets, warnings, key);
      if (paint) out.push(paint);
    }
    return out;
  }
  function toEffects(es) {
    var _a;
    if (!es) return [];
    const out = [];
    for (const e of es) {
      if (e.type === "DROP_SHADOW" || e.type === "INNER_SHADOW") {
        out.push({
          type: e.type,
          color: e.color,
          offset: e.offset,
          radius: e.radius,
          spread: (_a = e.spread) != null ? _a : 0,
          visible: true,
          blendMode: "NORMAL"
        });
      } else {
        out.push({ type: e.type, radius: e.radius, visible: true });
      }
    }
    return out;
  }
  var TYPE_OF_EMIT = {
    frame: ["FRAME"],
    group: ["FRAME", "GROUP"],
    text: ["TEXT"],
    rect: ["RECTANGLE"],
    ellipse: ["ELLIPSE"],
    line: ["LINE"],
    vector: ["VECTOR"],
    instance: ["INSTANCE"],
    component: ["COMPONENT"]
  };
  function hostsSvg(en, assets) {
    var _a;
    const f = ((_a = en.fills) != null ? _a : []).find((x) => x.type === "IMAGE");
    return !!(f == null ? void 0 : f.assetRef) && assets.svgs.has(f.assetRef);
  }
  async function createNode(op, warnings, assets, nodes) {
    const t = op.node.type;
    if ((t === "rect" || t === "ellipse" || t === "line" || t === "vector") && hostsSvg(op.node, assets)) {
      return figma.createFrame();
    }
    switch (t) {
      case "text":
        return figma.createText();
      case "rect":
        return figma.createRectangle();
      case "ellipse":
        return figma.createEllipse();
      case "line":
        return figma.createLine();
      case "vector":
        return figma.createVector();
      case "component":
        return figma.createComponent();
      case "instance": {
        const ref = op.node.instanceOf;
        if (ref) {
          const local = nodes.get(ref);
          if (local && local.type === "COMPONENT") return local.createInstance();
          try {
            const src = await figma.getNodeByIdAsync(ref);
            if (src && src.type === "COMPONENT") return src.createInstance();
          } catch (e) {
          }
          try {
            const comp = await figma.importComponentByKeyAsync(ref);
            return comp.createInstance();
          } catch (e) {
          }
        }
        warnings.push({ key: op.key, field: "instanceOf", message: `component "${ref}" unavailable \u2192 frame` });
        return figma.createFrame();
      }
      case "group":
      case "frame":
      default:
        return figma.createFrame();
    }
  }
  async function applyIntrinsic(node, op, assets, resolveFont, warnings) {
    const en = op.node;
    const key = op.key;
    tryset(warnings, key, "name", () => {
      var _a;
      node.name = (_a = en.name) != null ? _a : node.name;
    });
    tryset(warnings, key, "pluginData", () => node.setPluginData(PLUMB_KEY, key));
    if (en.type === "text" && en.text) {
      await applyText(node, en.text, en.size, en.fills, assets, resolveFont, warnings, key);
    }
    if (en.type === "vector" && en.vectorPaths) {
      tryset(warnings, key, "vectorPaths", () => {
        node.vectorPaths = en.vectorPaths.map((v) => {
          var _a;
          return {
            windingRule: (_a = v.windingRule) != null ? _a : "NONZERO",
            data: v.data
          };
        });
      });
    }
    const textFixed = en.type === "text" && (!en.text || en.text.autoResize === "NONE");
    if ((en.type !== "text" || textFixed) && "resize" in node && en.size) {
      tryset(
        warnings,
        key,
        "size",
        () => node.resize(
          Math.max(0.01, en.size.w),
          Math.max(0.01, en.size.h)
        )
      );
    }
    if ("fills" in node && en.fills !== void 0 && en.type !== "text") {
      tryset(warnings, key, "fills", () => {
        node.fills = toPaints(en.fills, assets, warnings, key);
      });
    } else if (en.fills === void 0 && (node.type === "FRAME" || node.type === "COMPONENT")) {
      tryset(warnings, key, "fills", () => {
        node.fills = [];
      });
    }
    if ("strokes" in node && en.strokes !== void 0) {
      tryset(warnings, key, "strokes", () => {
        node.strokes = toPaints(en.strokes, assets, warnings, key);
      });
    }
    if (en.strokeWeight !== void 0 && "strokeWeight" in node) {
      tryset(warnings, key, "strokeWeight", () => {
        node.strokeWeight = en.strokeWeight;
      });
    }
    if (en.strokeAlign && "strokeAlign" in node) {
      tryset(warnings, key, "strokeAlign", () => {
        node.strokeAlign = en.strokeAlign;
      });
    }
    if (en.dashPattern && "dashPattern" in node) {
      tryset(warnings, key, "dashPattern", () => {
        node.dashPattern = en.dashPattern;
      });
    }
    if (en.strokeSides) {
      const s = en.strokeSides;
      const an = node;
      tryset(warnings, key, "strokeSides", () => {
        an.strokeTopWeight = s.t;
        an.strokeRightWeight = s.r;
        an.strokeBottomWeight = s.b;
        an.strokeLeftWeight = s.l;
      });
    }
    if (en.cornerRadius !== void 0) applyRadius(node, en.cornerRadius, warnings, key);
    if (en.effects !== void 0 && "effects" in node) {
      tryset(warnings, key, "effects", () => {
        node.effects = toEffects(en.effects);
      });
    }
    if (en.opacity !== void 0 && "opacity" in node) {
      tryset(warnings, key, "opacity", () => {
        node.opacity = en.opacity;
      });
    }
    if (en.blendMode && "blendMode" in node) {
      tryset(warnings, key, "blendMode", () => {
        node.blendMode = en.blendMode;
      });
    }
    if (en.clip !== void 0 && "clipsContent" in node) {
      tryset(warnings, key, "clip", () => {
        node.clipsContent = en.clip;
      });
    }
    if (en.rotation !== void 0 && "rotation" in node) {
      tryset(warnings, key, "rotation", () => {
        node.rotation = en.rotation;
      });
    }
    const dim = node;
    if (en.minWidth !== void 0) tryset(warnings, key, "minWidth", () => dim.minWidth = en.minWidth);
    if (en.maxWidth !== void 0) tryset(warnings, key, "maxWidth", () => dim.maxWidth = en.maxWidth);
    if (en.minHeight !== void 0) tryset(warnings, key, "minHeight", () => dim.minHeight = en.minHeight);
    if (en.maxHeight !== void 0) tryset(warnings, key, "maxHeight", () => dim.maxHeight = en.maxHeight);
    if (en.type === "instance" && en.componentProps && node.type === "INSTANCE") {
      tryset(
        warnings,
        key,
        "componentProps",
        () => node.setProperties(en.componentProps)
      );
    }
  }
  function applyRadius(node, r, warnings, key) {
    const an = node;
    if (typeof r === "number") {
      if ("cornerRadius" in node) tryset(warnings, key, "cornerRadius", () => an.cornerRadius = r);
      return;
    }
    tryset(warnings, key, "cornerRadius", () => {
      an.topLeftRadius = r[0];
      an.topRightRadius = r[1];
      an.bottomRightRadius = r[2];
      an.bottomLeftRadius = r[3];
    });
  }
  async function applyText(node, txt, size, baseFills, assets, resolveFont, warnings, key) {
    const base = resolveFont(txt.font);
    try {
      node.fontName = { family: base.family, style: base.style };
    } catch (e) {
      warnings.push({ key, field: "fontName", message: errText(e) });
    }
    tryset(warnings, key, "characters", () => {
      node.characters = txt.characters;
    });
    tryset(warnings, key, "fontSize", () => {
      node.fontSize = txt.fontSize;
    });
    if (txt.lineHeightPx !== void 0) {
      tryset(warnings, key, "lineHeight", () => {
        node.lineHeight = { value: txt.lineHeightPx, unit: "PIXELS" };
      });
    }
    if (txt.letterSpacing !== void 0) {
      tryset(warnings, key, "letterSpacing", () => {
        node.letterSpacing = { value: txt.letterSpacing, unit: "PIXELS" };
      });
    }
    if (txt.align) {
      tryset(warnings, key, "align", () => {
        node.textAlignHorizontal = txt.align;
      });
    }
    if (txt.decoration) {
      tryset(warnings, key, "decoration", () => {
        node.textDecoration = txt.decoration;
      });
    }
    if (txt.autoResize) {
      if (txt.autoResize === "TRUNCATE") {
        tryset(warnings, key, "autoResize", () => {
          node.textAutoResize = "HEIGHT";
          node.textTruncate = "ENDING";
        });
      } else {
        tryset(warnings, key, "autoResize", () => {
          node.textAutoResize = txt.autoResize;
        });
      }
    }
    if (size && (txt.autoResize === "HEIGHT" || txt.autoResize === "TRUNCATE")) {
      tryset(
        warnings,
        key,
        "textWidth",
        () => node.resize(Math.max(0.01, size.w), Math.max(0.01, node.height))
      );
    }
    if (baseFills !== void 0) {
      tryset(warnings, key, "fills", () => {
        node.fills = toPaints(baseFills, assets, warnings, key);
      });
    }
    if (txt.runs && txt.runs.length) {
      for (const run of txt.runs) {
        const lo = Math.max(0, run.start);
        const hi = Math.min(txt.characters.length, run.end);
        if (hi <= lo) continue;
        if (run.font) {
          const rf = await ensureFace(resolveFont(run.font), resolveFont, warnings);
          tryset(
            warnings,
            key,
            "run.font",
            () => node.setRangeFontName(lo, hi, { family: rf.family, style: rf.style })
          );
        }
        if (run.fontSize !== void 0)
          tryset(warnings, key, "run.fontSize", () => node.setRangeFontSize(lo, hi, run.fontSize));
        if (run.fills)
          tryset(
            warnings,
            key,
            "run.fills",
            () => node.setRangeFills(lo, hi, toPaints(run.fills, assets, warnings, key))
          );
        if (run.lineHeightPx !== void 0)
          tryset(
            warnings,
            key,
            "run.lineHeight",
            () => node.setRangeLineHeight(lo, hi, { value: run.lineHeightPx, unit: "PIXELS" })
          );
        if (run.letterSpacing !== void 0)
          tryset(
            warnings,
            key,
            "run.letterSpacing",
            () => node.setRangeLetterSpacing(lo, hi, { value: run.letterSpacing, unit: "PIXELS" })
          );
        if (run.decoration)
          tryset(
            warnings,
            key,
            "run.decoration",
            () => node.setRangeTextDecoration(lo, hi, run.decoration)
          );
      }
    }
  }
  function materializeSvg(node, op, assets, warnings) {
    var _a, _b;
    const en = op.node;
    const imgFill = ((_a = en.fills) != null ? _a : []).find((f) => f.type === "IMAGE");
    if (!imgFill) return;
    const svg = assets.svgs.get(imgFill.assetRef);
    if (!svg) return;
    if (!("appendChild" in node)) {
      warnings.push({ key: op.key, field: "svg", message: "svg target is not a container" });
      return;
    }
    try {
      if ("children" in node) {
        for (const child of [...node.children]) child.remove();
      }
      if ("fills" in node) node.fills = [];
      if ("clipsContent" in node && en.clip === void 0) node.clipsContent = false;
      const frame = figma.createNodeFromSvg(svg);
      frame.name = (_b = en.name) != null ? _b : frame.name;
      frame.resize(Math.max(0.01, en.size.w), Math.max(0.01, en.size.h));
      node.appendChild(frame);
      frame.x = 0;
      frame.y = 0;
    } catch (e) {
      warnings.push({ key: op.key, field: "svg", message: errText(e) });
    }
  }
  function applyContainerLayout(node, en, warnings, key) {
    const lay = en.layout;
    if (!lay || !("layoutMode" in node)) return;
    const f = node;
    tryset(warnings, key, "layout", () => {
      f.layoutMode = lay.mode;
      if (lay.gap !== void 0) f.itemSpacing = lay.gap;
      f.paddingTop = lay.pad.t;
      f.paddingRight = lay.pad.r;
      f.paddingBottom = lay.pad.b;
      f.paddingLeft = lay.pad.l;
      if (lay.primary) f.primaryAxisAlignItems = lay.primary;
      if (lay.counter) f.counterAxisAlignItems = lay.counter;
      if (lay.wrap) {
        f.layoutWrap = "WRAP";
        if (lay.gapCross !== void 0) f.counterAxisSpacing = lay.gapCross;
      }
    });
  }
  function applyChildLayout(node, en, warnings, key) {
    const c = en.child;
    if (!c) return;
    const parent = node.parent;
    const inAutoLayout = !!parent && !!parent.layoutMode && parent.layoutMode !== "NONE";
    const self = node;
    const canHug = !!self.layoutMode && self.layoutMode !== "NONE" || node.type === "TEXT";
    const applySizing = (field, prop, val) => {
      if (!val) return;
      const allowed = val === "HUG" ? canHug : inAutoLayout;
      if (!allowed) return;
      tryset(warnings, key, field, () => {
        node[prop] = val;
      });
    };
    applySizing("sizingH", "layoutSizingHorizontal", c.sizingH);
    applySizing("sizingV", "layoutSizingVertical", c.sizingV);
    if (!inAutoLayout) return;
    if (c.grow !== void 0)
      tryset(warnings, key, "grow", () => node.layoutGrow = c.grow);
    if (c.align)
      tryset(
        warnings,
        key,
        "align",
        () => node.layoutAlign = c.align
      );
  }
  async function resolveTarget(target, warnings) {
    var _a, _b;
    if (target.kind === "into") {
      const n = await figma.getNodeByIdAsync(target.nodeId);
      if (n && "appendChild" in n) {
        return { container: n, replaced: null, pos: null };
      }
      warnings.push({ key: "@target", field: "into", message: "target node not found \u2192 current page" });
      return { container: figma.currentPage, replaced: null, pos: null };
    }
    if (target.kind === "replace") {
      const n = await figma.getNodeByIdAsync(target.nodeId);
      if (n && n.parent && "appendChild" in n.parent) {
        const sn = n;
        return {
          container: n.parent,
          replaced: sn,
          pos: "x" in sn ? { x: sn.x, y: sn.y } : null
        };
      }
      warnings.push({ key: "@target", field: "replace", message: "target node not found \u2192 current page" });
      return { container: figma.currentPage, replaced: null, pos: null };
    }
    if (target.pageName) {
      let page = figma.root.children.find(
        (p) => p.type === "PAGE" && p.name === target.pageName
      );
      if (!page) {
        page = figma.createPage();
        page.name = target.pageName;
      }
      try {
        await page.loadAsync();
      } catch (e) {
      }
      try {
        await figma.setCurrentPageAsync(page);
      } catch (e) {
      }
      return { container: page, replaced: null, pos: (_a = target.pos) != null ? _a : { x: 0, y: 0 } };
    }
    return {
      container: figma.currentPage,
      replaced: null,
      pos: (_b = target.pos) != null ? _b : { x: 0, y: 0 }
    };
  }
  function indexByKey(container) {
    const map = /* @__PURE__ */ new Map();
    function walk(n) {
      if (n.getPluginData) {
        const k = n.getPluginData(PLUMB_KEY);
        if (k) map.set(k, n);
      }
      if ("children" in n) {
        for (const c of n.children) walk(c);
      }
    }
    for (const c of container.children) walk(c);
    return map;
  }
  function emitTypeMatches(node, t) {
    var _a;
    return ((_a = TYPE_OF_EMIT[t]) != null ? _a : []).indexOf(node.type) !== -1;
  }
  async function applyDesign(plan, onProgress) {
    var _a, _b, _c;
    const warnings = [];
    let created = 0;
    let updated = 0;
    let deleted = 0;
    const resolveFont = await loadFonts(plan.fonts, warnings, onProgress);
    const assets = hydrateAssets(plan.assets, warnings, onProgress);
    const target = await resolveTarget(plan.target, warnings);
    const existing = plan.mode === "sync" ? indexByKey(target.container) : /* @__PURE__ */ new Map();
    const usedExisting = /* @__PURE__ */ new Set();
    const nodes = /* @__PURE__ */ new Map();
    const opByKey = /* @__PURE__ */ new Map();
    const total = plan.ops.length;
    let i = 0;
    for (const op of plan.ops) {
      opByKey.set(op.key, op);
      let node;
      const reuse = existing.get(op.key);
      const reuseAlive = !!reuse && !reuse.removed;
      if (reuseAlive && emitTypeMatches(reuse, op.node.type)) {
        node = reuse;
        usedExisting.add(op.key);
        updated += 1;
      } else {
        if (reuseAlive) {
          tryset(warnings, op.key, "recreate", () => reuse.remove());
        }
        try {
          node = await createNode(op, warnings, assets, nodes);
        } catch (e) {
          warnings.push({ key: op.key, field: "create", message: errText(e) });
          node = figma.createFrame();
        }
        created += 1;
      }
      nodes.set(op.key, node);
      await applyIntrinsic(node, op, assets, resolveFont, warnings);
      materializeSvg(node, op, assets, warnings);
      i += 1;
      onProgress == null ? void 0 : onProgress("nodes", i, total);
    }
    let firstRoot = null;
    for (const op of plan.ops) {
      const node = nodes.get(op.key);
      if (op.parent === null) {
        if (!firstRoot) firstRoot = op;
        tryset(warnings, op.key, "parent", () => target.container.appendChild(node));
      } else {
        const parent = nodes.get(op.parent);
        if (parent && "appendChild" in parent) {
          tryset(warnings, op.key, "parent", () => parent.appendChild(node));
        } else {
          warnings.push({ key: op.key, field: "parent", message: `parent "${op.parent}" missing` });
          tryset(warnings, op.key, "parent", () => target.container.appendChild(node));
        }
      }
    }
    for (const op of plan.ops) applyContainerLayout(nodes.get(op.key), op.node, warnings, op.key);
    for (const op of plan.ops) applyChildLayout(nodes.get(op.key), op.node, warnings, op.key);
    onProgress == null ? void 0 : onProgress("layout", total, total);
    for (const op of plan.ops) {
      const en = op.node;
      const node = nodes.get(op.key);
      const parentIsAuto = op.parent ? !!((_a = nodes.get(op.parent)) == null ? void 0 : _a.layoutMode) && nodes.get(op.parent).layoutMode !== "NONE" : false;
      if (op.parent === null) {
        const p = (_b = target.pos) != null ? _b : { x: 0, y: 0 };
        const rel = (_c = en.pos) != null ? _c : { x: 0, y: 0 };
        tryset(warnings, op.key, "pos", () => {
          node.x = p.x + rel.x;
          node.y = p.y + rel.y;
        });
      } else if (en.pos && !parentIsAuto) {
        tryset(warnings, op.key, "pos", () => {
          node.x = en.pos.x;
          node.y = en.pos.y;
        });
      }
      if (en.absolute) {
        tryset(warnings, op.key, "absolute", () => {
          node.layoutPositioning = "ABSOLUTE";
        });
      }
    }
    if (plan.mode === "sync" && plan.prune) {
      for (const [k, node] of existing) {
        if (!usedExisting.has(k) && !nodes.has(k)) {
          tryset(warnings, k, "prune", () => {
            node.remove();
            deleted += 1;
          });
        }
      }
    }
    if (target.replaced && !nodes.has(target.replaced.id)) {
      tryset(warnings, "@target", "replace", () => {
        target.replaced.remove();
        deleted += 1;
      });
    }
    onProgress == null ? void 0 : onProgress("finalize", total, total);
    const root = firstRoot ? nodes.get(firstRoot.key) : null;
    if (plan.reveal && root) {
      tryset(warnings, "@reveal", "reveal", () => {
        figma.currentPage.selection = [root];
        figma.viewport.scrollAndZoomIntoView([root]);
      });
    }
    const ids = {};
    for (const [k, n] of nodes) ids[k] = n.id;
    return {
      rootId: root ? root.id : "",
      rootKey: firstRoot ? firstRoot.key : "",
      created,
      updated,
      deleted,
      ids,
      warnings
    };
  }

  // figma-plugin/foundations.ts
  var STYLE_FOR_WEIGHT = {
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
  var FALLBACK_FAMILY = "Inter";
  function styleForWeight(w) {
    var _a;
    const rounded = Math.round(w / 100) * 100;
    return (_a = STYLE_FOR_WEIGHT[rounded]) != null ? _a : "Regular";
  }
  function hexToRgba(hex) {
    let h = hex.trim().replace(/^#/, "");
    if (h.length === 3) h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2];
    if (h.length === 4)
      h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2] + h[3] + h[3];
    const r = parseInt(h.slice(0, 2), 16) / 255;
    const g = parseInt(h.slice(2, 4), 16) / 255;
    const b = parseInt(h.slice(4, 6), 16) / 255;
    const a = h.length >= 8 ? parseInt(h.slice(6, 8), 16) / 255 : 1;
    return {
      r: isNaN(r) ? 0 : r,
      g: isNaN(g) ? 0 : g,
      b: isNaN(b) ? 0 : b,
      a: isNaN(a) ? 1 : a
    };
  }
  function errText2(e) {
    return e instanceof Error ? e.message : String(e);
  }
  async function ensureCollection(spec, warnings) {
    var _a;
    const all = await figma.variables.getLocalVariableCollectionsAsync();
    let collection = (_a = all.find((c) => c.name === spec.name)) != null ? _a : null;
    if (!collection) collection = figma.variables.createVariableCollection(spec.name);
    const modeIds = {};
    const wanted = spec.modes.length ? spec.modes : ["Mode 1"];
    const existingModes = collection.modes.slice();
    wanted.forEach((name, idx) => {
      const found = existingModes.find((m) => m.name === name);
      if (found) {
        modeIds[name] = found.modeId;
        return;
      }
      if (idx === 0 && existingModes.length) {
        try {
          collection.renameMode(existingModes[0].modeId, name);
          modeIds[name] = existingModes[0].modeId;
          return;
        } catch (e) {
        }
      }
      try {
        modeIds[name] = collection.addMode(name);
      } catch (e) {
        warnings.push(`mode "${name}" in "${spec.name}": ${errText2(e)}`);
      }
    });
    const existingVars = await figma.variables.getLocalVariablesAsync();
    const byName = /* @__PURE__ */ new Map();
    for (const spv of spec.variables) {
      let v = existingVars.find(
        (x) => x.name === spv.name && x.variableCollectionId === collection.id
      );
      if (!v) {
        try {
          v = figma.variables.createVariable(spv.name, collection, spv.type);
        } catch (e) {
          warnings.push(`variable "${spv.name}": ${errText2(e)}`);
          continue;
        }
      }
      if (spv.scopes && spv.scopes.length) {
        try {
          v.scopes = spv.scopes;
        } catch (e) {
        }
      }
      byName.set(spv.name, v);
    }
    return { spec, collection, modeIds, variables: byName };
  }
  function setVariableValues(built, nameIndex, warnings) {
    for (const spv of built.spec.variables) {
      const v = built.variables.get(spv.name);
      if (!v) continue;
      for (const [modeName, value] of Object.entries(spv.values)) {
        const modeId = built.modeIds[modeName];
        if (!modeId) {
          warnings.push(`variable "${spv.name}" references unknown mode "${modeName}"`);
          continue;
        }
        try {
          if ("alias" in value) {
            const target = nameIndex.get(value.alias);
            if (!target) {
              warnings.push(`alias "${value.alias}" (from "${spv.name}") not found`);
              continue;
            }
            v.setValueForMode(modeId, figma.variables.createVariableAlias(target));
          } else if ("hex" in value) {
            v.setValueForMode(modeId, hexToRgba(value.hex));
          } else {
            v.setValueForMode(modeId, value.number);
          }
        } catch (e) {
          warnings.push(`set "${spv.name}"[${modeName}]: ${errText2(e)}`);
        }
      }
    }
  }
  async function ensureTextStyles(specs, nameIndex, warnings) {
    var _a;
    const ids = {};
    const existing = await figma.getLocalTextStylesAsync();
    for (const s of specs) {
      let style = (_a = existing.find((x) => x.name === s.name)) != null ? _a : null;
      if (!style) style = figma.createTextStyle();
      style.name = s.name;
      const desiredStyle = styleForWeight(s.weight);
      let face = { family: s.family, style: desiredStyle };
      try {
        await figma.loadFontAsync(face);
      } catch (e) {
        face = { family: FALLBACK_FAMILY, style: "Regular" };
        try {
          await figma.loadFontAsync(face);
          warnings.push(`text style "${s.name}": "${s.family} ${desiredStyle}" \u2192 Inter Regular`);
        } catch (e2) {
          warnings.push(`text style "${s.name}" font load failed: ${errText2(e2)}`);
        }
      }
      try {
        style.fontName = face;
        style.fontSize = s.sizePx;
        if (s.lineHeightPercent !== void 0)
          style.lineHeight = { value: s.lineHeightPercent, unit: "PERCENT" };
        if (s.letterSpacingPx !== void 0)
          style.letterSpacing = { value: s.letterSpacingPx, unit: "PIXELS" };
        if (s.textCase) style.textCase = s.textCase === "UPPER" ? "UPPER" : "ORIGINAL";
      } catch (e) {
        warnings.push(`text style "${s.name}": ${errText2(e)}`);
      }
      if (s.boundVars) {
        if (s.boundVars.fontSize) bindStyleVar(style, "fontSize", s.boundVars.fontSize, nameIndex, warnings);
        if (s.boundVars.lineHeight)
          bindStyleVar(style, "lineHeight", s.boundVars.lineHeight, nameIndex, warnings);
      }
      ids[s.name] = style.id;
    }
    return ids;
  }
  function bindStyleVar(style, field, varName, nameIndex, warnings) {
    const v = nameIndex.get(varName);
    if (!v) {
      warnings.push(`text style bound var "${varName}" not found`);
      return;
    }
    try {
      style.setBoundVariable(field, v);
    } catch (e) {
      warnings.push(`bind "${varName}" \u2192 ${field}: ${errText2(e)}`);
    }
  }
  async function ensureEffectStyles(specs, warnings) {
    var _a;
    const ids = {};
    const existing = await figma.getLocalEffectStylesAsync();
    for (const s of specs) {
      let style = (_a = existing.find((x) => x.name === s.name)) != null ? _a : null;
      if (!style) style = figma.createEffectStyle();
      style.name = s.name;
      try {
        style.effects = s.effects.map((e) => {
          const c = hexToRgba(e.color);
          return {
            type: e.type === "inner-shadow" ? "INNER_SHADOW" : "DROP_SHADOW",
            color: c,
            offset: { x: e.x, y: e.y },
            radius: e.blur,
            spread: e.spread,
            visible: true,
            blendMode: "NORMAL"
          };
        });
      } catch (e) {
        warnings.push(`effect style "${s.name}": ${errText2(e)}`);
      }
      ids[s.name] = style.id;
    }
    return ids;
  }
  async function ensureGridStyles(specs, warnings) {
    var _a, _b, _c, _d, _e, _f;
    const ids = {};
    const existing = await figma.getLocalGridStylesAsync();
    for (const s of specs) {
      let style = (_a = existing.find((x) => x.name === s.name)) != null ? _a : null;
      if (!style) style = figma.createGridStyle();
      style.name = s.name;
      try {
        const grid = s.pattern === "GRID" ? {
          pattern: "GRID",
          sectionSize: (_b = s.sectionSizePx) != null ? _b : 8,
          visible: true,
          color: { r: 1, g: 0, b: 0, a: 0.1 }
        } : {
          pattern: s.pattern,
          alignment: (_c = s.alignment) != null ? _c : "STRETCH",
          gutterSize: (_d = s.gutterPx) != null ? _d : 20,
          count: (_e = s.count) != null ? _e : 12,
          offset: (_f = s.marginPx) != null ? _f : 0,
          sectionSize: s.sectionSizePx,
          visible: true,
          color: { r: 1, g: 0, b: 0, a: 0.1 }
        };
        style.layoutGrids = [grid];
      } catch (e) {
        warnings.push(`grid style "${s.name}": ${errText2(e)}`);
      }
      ids[s.name] = style.id;
    }
    return ids;
  }
  function dryRunResult(plan) {
    return {
      collections: plan.collections.map((c) => ({
        name: c.name,
        id: "",
        modeIds: {},
        variableIds: {}
      })),
      textStyleIds: {},
      effectStyleIds: {},
      gridStyleIds: {},
      warnings: [
        `dry run: would create ${plan.collections.length} collection(s), ${plan.collections.reduce((n, c) => n + c.variables.length, 0)} variable(s), ${plan.textStyles.length} text / ${plan.effectStyles.length} effect / ${plan.gridStyles.length} grid style(s)`
      ]
    };
  }
  async function applyFoundations(plan, dryRun) {
    if (dryRun) return dryRunResult(plan);
    const warnings = [];
    const built = [];
    const nameIndex = /* @__PURE__ */ new Map();
    for (const spec of plan.collections) {
      const bc = await ensureCollection(spec, warnings);
      built.push(bc);
      for (const [name, v] of bc.variables) nameIndex.set(name, v);
    }
    for (const bc of built) setVariableValues(bc, nameIndex, warnings);
    const textStyleIds = await ensureTextStyles(plan.textStyles, nameIndex, warnings);
    const effectStyleIds = await ensureEffectStyles(plan.effectStyles, warnings);
    const gridStyleIds = await ensureGridStyles(plan.gridStyles, warnings);
    return {
      collections: built.map((bc) => {
        const variableIds = {};
        for (const [name, v] of bc.variables) variableIds[name] = v.id;
        return {
          name: bc.spec.name,
          id: bc.collection.id,
          modeIds: bc.modeIds,
          variableIds
        };
      }),
      textStyleIds,
      effectStyleIds,
      gridStyleIds,
      warnings
    };
  }

  // figma-plugin/motion-emit.ts
  var NAMED_EASING = {
    linear: "LINEAR",
    "ease-in": "EASE_IN",
    "ease-out": "EASE_OUT",
    "ease-in-out": "EASE_IN_AND_OUT",
    "ease-in-and-out": "EASE_IN_AND_OUT",
    "ease-in-back": "EASE_IN_BACK",
    "ease-out-back": "EASE_OUT_BACK",
    "ease-in-out-back": "EASE_IN_AND_OUT_BACK",
    "ease-in-and-out-back": "EASE_IN_AND_OUT_BACK",
    gentle: "GENTLE",
    quick: "QUICK",
    bouncy: "BOUNCY",
    slow: "SLOW"
  };
  function parseFloats(csv) {
    return csv.split(",").map((s) => parseFloat(s.trim())).filter((n) => Number.isFinite(n));
  }
  function parseEasing(easing) {
    var _a;
    if (!easing) return { type: "EASE_OUT" };
    if (easing.indexOf("bezier:") === 0) {
      const [x1, y1, x2, y2] = parseFloats(easing.slice(7));
      if ([x1, y1, x2, y2].every((n) => typeof n === "number")) {
        return {
          type: "CUSTOM_CUBIC_BEZIER",
          easingFunctionCubicBezier: { x1, y1, x2, y2 }
        };
      }
      return { type: "EASE_OUT" };
    }
    if (easing.indexOf("spring:") === 0) {
      const [mass, stiffness, damping] = parseFloats(easing.slice(7));
      if ([mass, stiffness, damping].every((n) => typeof n === "number")) {
        return {
          type: "CUSTOM_SPRING",
          easingFunctionSpring: {
            mass,
            stiffness,
            damping,
            initialVelocity: 0
          }
        };
      }
      return { type: "GENTLE" };
    }
    return { type: (_a = NAMED_EASING[easing]) != null ? _a : "EASE_OUT" };
  }
  var SIMPLE_KINDS = ["DISSOLVE", "SMART_ANIMATE", "SCROLL_ANIMATE"];
  var DIRECTIONAL_KINDS = [
    "MOVE_IN",
    "MOVE_OUT",
    "PUSH",
    "SLIDE_IN",
    "SLIDE_OUT"
  ];
  function buildTransition(spec) {
    var _a, _b;
    const kind = spec.kind;
    if (!kind || kind === "INSTANT") return null;
    const easing = parseEasing(spec.easing);
    const duration = (typeof spec.durationMs === "number" ? spec.durationMs : 300) / 1e3;
    if (SIMPLE_KINDS.indexOf(kind) !== -1) {
      return { type: kind, easing, duration };
    }
    if (DIRECTIONAL_KINDS.indexOf(kind) !== -1) {
      return {
        type: kind,
        direction: (_a = spec.direction) != null ? _a : "LEFT",
        matchLayers: (_b = spec.matchLayers) != null ? _b : false,
        easing,
        duration
      };
    }
    return null;
  }
  function buildTrigger(spec) {
    var _a, _b;
    switch (spec.trigger) {
      case "ON_CLICK":
      case "ON_HOVER":
      case "ON_PRESS":
      case "ON_DRAG":
        return { type: spec.trigger };
      case "AFTER_TIMEOUT":
        return { type: "AFTER_TIMEOUT", timeout: ((_a = spec.timeoutMs) != null ? _a : 0) / 1e3 };
      case "MOUSE_UP":
      case "MOUSE_DOWN":
        return { type: spec.trigger, delay: 0 };
      case "MOUSE_ENTER":
      case "MOUSE_LEAVE":
        return { type: spec.trigger, delay: 0, deprecatedVersion: false };
      case "ON_KEY_DOWN":
        return { type: "ON_KEY_DOWN", device: "KEYBOARD", keyCodes: (_b = spec.keys) != null ? _b : [] };
      default:
        return { type: "ON_CLICK" };
    }
  }
  function variableData(value) {
    if (typeof value === "boolean") return { type: "BOOLEAN", resolvedType: "BOOLEAN", value };
    if (typeof value === "number") return { type: "FLOAT", resolvedType: "FLOAT", value };
    return { type: "STRING", resolvedType: "STRING", value };
  }
  var NODE_NAVS = ["NAVIGATE", "SWAP", "OVERLAY", "SCROLL_TO"];
  function buildActions(spec, destId, overlayAt) {
    var _a;
    switch (spec.navigation) {
      case "BACK":
        return [{ type: "BACK" }];
      case "CLOSE":
        return [{ type: "CLOSE" }];
      case "URL":
        return spec.url ? [{ type: "URL", url: spec.url }] : [];
      case "SET_VAR": {
        const vars = (_a = spec.setVars) != null ? _a : {};
        const actions = [];
        for (const [variableId, value] of Object.entries(vars)) {
          actions.push({ type: "SET_VARIABLE", variableId, variableValue: variableData(value) });
        }
        return actions;
      }
      default: {
        if (NODE_NAVS.indexOf(spec.navigation) === -1) return [];
        if (!destId) return [];
        const action = {
          type: "NODE",
          destinationId: destId,
          navigation: spec.navigation,
          transition: buildTransition(spec)
        };
        if (spec.navigation === "OVERLAY" && overlayAt) {
          action.overlayRelativePosition = { x: overlayAt.x, y: overlayAt.y };
        }
        if (spec.preserveScroll !== void 0) action.resetScrollPosition = !spec.preserveScroll;
        if (spec.resetState) action.resetInteractiveComponents = true;
        return [action];
      }
    }
  }
  async function nodeForEl(el, idMap) {
    const id = idMap.get(el);
    if (!id) return null;
    try {
      return await figma.getNodeByIdAsync(id);
    } catch (e) {
      return null;
    }
  }
  function hexToRgba2(hex) {
    let h = hex.trim().replace(/^#/, "");
    if (h.length === 3) {
      h = h[0] + h[0] + h[1] + h[1] + h[2] + h[2];
    }
    const r = parseInt(h.slice(0, 2), 16) / 255;
    const g = parseInt(h.slice(2, 4), 16) / 255;
    const b = parseInt(h.slice(4, 6), 16) / 255;
    const a = h.length >= 8 ? parseInt(h.slice(6, 8), 16) / 255 : 1;
    return {
      r: Number.isFinite(r) ? r : 0,
      g: Number.isFinite(g) ? g : 0,
      b: Number.isFinite(b) ? b : 0,
      a: Number.isFinite(a) ? a : 1
    };
  }
  var OVERFLOW_DIR = {
    NONE: "NONE",
    HORIZONTAL: "HORIZONTAL",
    VERTICAL: "VERTICAL",
    BOTH: "BOTH"
  };
  async function wireMotion(plan, idMap) {
    var _a;
    const misses = [];
    let wired = 0;
    const overlayByEl = /* @__PURE__ */ new Map();
    for (const frame of plan.frames) {
      if (frame.overlay) overlayByEl.set(frame.el, frame.overlay);
    }
    try {
      for (const binding of plan.bindings) {
        const source = await nodeForEl(binding.sourceEl, idMap);
        if (!source) {
          misses.push(`source el "${binding.sourceEl}" not in idMap`);
          continue;
        }
        if (!("setReactionsAsync" in source)) {
          misses.push(`node "${binding.sourceEl}" does not support reactions`);
          continue;
        }
        const reactions = [];
        for (const spec of binding.specs) {
          let destId = null;
          if (spec.target !== void 0 && NODE_NAVS.indexOf(spec.navigation) !== -1) {
            destId = (_a = idMap.get(spec.target)) != null ? _a : null;
            if (!destId) misses.push(`target el "${spec.target}" not in idMap`);
          }
          const overlayAt = spec.navigation === "OVERLAY" && spec.target ? overlayAtFor(overlayByEl.get(spec.target)) : void 0;
          const actions = buildActions(spec, destId, overlayAt);
          if (actions.length === 0) continue;
          reactions.push({ trigger: buildTrigger(spec), actions });
        }
        try {
          await source.setReactionsAsync(reactions);
          wired += reactions.length;
        } catch (e) {
          misses.push(`setReactions failed on "${binding.sourceEl}": ${String(e)}`);
        }
      }
      for (const frame of plan.frames) {
        const node = await nodeForEl(frame.el, idMap);
        if (!node) {
          misses.push(`frame el "${frame.el}" not in idMap`);
          continue;
        }
        if (frame.overflow && "overflowDirection" in node) {
          try {
            node.overflowDirection = OVERFLOW_DIR[frame.overflow];
          } catch (e) {
            misses.push(`overflow not settable on "${frame.el}"`);
          }
        }
        if (frame.overlay) applyOverlayPresentation(node, frame.overlay, frame.el, misses);
      }
      if (plan.prototype) applyPrototype(plan.prototype, idMap, misses);
      return { wired, misses, error: null };
    } catch (e) {
      return { wired, misses, error: e instanceof Error ? e.message : String(e) };
    }
  }
  function overlayAtFor(overlay) {
    if (overlay && overlay.position === "MANUAL" && overlay.at) return overlay.at;
    return void 0;
  }
  function applyOverlayPresentation(node, overlay, el, misses) {
    const writable = node;
    try {
      if (overlay.position && overlay.position !== "MANUAL") {
        writable.overlayPositionType = overlay.position;
      }
      if (overlay.backdrop) {
        writable.overlayBackground = { type: "SOLID_COLOR", color: hexToRgba2(overlay.backdrop) };
      }
      if (overlay.closeOnClickOutside !== void 0) {
        writable.overlayBackgroundInteraction = overlay.closeOnClickOutside ? "CLOSE_ON_CLICK_OUTSIDE" : "NONE";
      }
    } catch (e) {
      misses.push(`overlay presentation not settable on "${el}" (read-only in plugin API)`);
    }
  }
  function applyPrototype(proto, idMap, misses) {
    const page = figma.currentPage;
    if (proto.starts.length > 0) {
      const points = [];
      for (const start2 of proto.starts) {
        const id = idMap.get(start2.el);
        if (id) points.push({ nodeId: id, name: start2.name });
        else misses.push(`flow start el "${start2.el}" not in idMap`);
      }
      if (points.length > 0) {
        try {
          page.flowStartingPoints = points;
        } catch (e) {
          misses.push("flow starting points not settable");
        }
      }
    }
    if (proto.background) {
      try {
        page.prototypeBackgrounds = [
          { type: "SOLID", color: rgbOf(hexToRgba2(proto.background)) }
        ];
      } catch (e) {
        misses.push("prototype background not settable");
      }
    }
    if (proto.device && proto.device.kind !== "none") {
      misses.push("prototype device is not settable via the plugin API");
    }
  }
  function rgbOf(rgba) {
    return { r: rgba.r, g: rgba.g, b: rgba.b };
  }

  // figma-plugin/code.ts
  var PLUGIN_VERSION = "0.13.2";
  var PANEL = { w: 240, h: 144 };
  var DOT = { w: 60, h: 60 };
  figma.showUI(__html__, {
    width: PANEL.w,
    height: PANEL.h,
    title: "Plumb",
    themeColors: true
    // inherit Figma's light/dark theme via CSS variables
  });
  var WEIGHTS = {
    thin: 100,
    hairline: 100,
    extralight: 200,
    ultralight: 200,
    light: 300,
    normal: 400,
    regular: 400,
    book: 400,
    medium: 500,
    semibold: 600,
    demibold: 600,
    bold: 700,
    extrabold: 800,
    ultrabold: 800,
    black: 900,
    heavy: 900
  };
  function weightOf(styleName) {
    var _a;
    return (_a = WEIGHTS[styleName.toLowerCase().replace(/\s+|italic|oblique/g, "")]) != null ? _a : 400;
  }
  function handlesFromTransform(t) {
    if (!t || t.length !== 2) return void 0;
    const [a, b, c] = t[0];
    const [d, e, f] = t[1];
    const det = a * e - b * d;
    if (det === 0) return void 0;
    const inv = (x, y) => {
      const px = (e * (x - c) - b * (y - f)) / det;
      const py = (a * (y - f) - d * (x - c)) / det;
      return { x: px, y: py };
    };
    return [inv(0, 0), inv(1, 0), inv(0, 1)];
  }
  function normalizePaint(p, varName) {
    if (!p || typeof p !== "object") return p;
    const src = p;
    const out = __spreadValues({}, src);
    if (typeof src.imageHash === "string" && !src.imageRef) out.imageRef = src.imageHash;
    if (Array.isArray(src.gradientTransform) && !src.gradientHandlePositions) {
      const hp = handlesFromTransform(
        src.gradientTransform
      );
      if (hp) out.gradientHandlePositions = hp;
    }
    if (varName) out.var = varName;
    return out;
  }
  var variableMapCache = null;
  async function buildVariableMap() {
    if (variableMapCache) return variableMapCache;
    const map = /* @__PURE__ */ new Map();
    try {
      const vars = await figma.variables.getLocalVariablesAsync();
      for (const v of vars) map.set(v.id, v.name);
    } catch (e) {
    }
    variableMapCache = map;
    return map;
  }
  function invalidateVariableMapCache() {
    variableMapCache = null;
  }
  function varNameFor(binding, varMap) {
    if (!binding || !varMap) return void 0;
    const id = binding.id;
    if (typeof id !== "string") return void 0;
    return varMap.get(id);
  }
  function serializeReactions(node) {
    const r = node.reactions;
    if (!Array.isArray(r) || r.length === 0) return void 0;
    return r.map((entry) => {
      if (!entry || typeof entry !== "object") return entry;
      const _a = entry, { actions: _drop } = _a, rest = __objRest(_a, ["actions"]);
      void _drop;
      const action = rest.action;
      if (action) {
        const overlayPos = action.overlayRelativePosition;
        const overlayBg = action.overlayBackground;
        const out = __spreadValues({}, action);
        if (overlayPos) out.overlayRelativePosition = overlayPos;
        if (overlayBg) out.overlayBackground = overlayBg;
        rest.action = out;
      }
      return rest;
    });
  }
  function serialize(node, varMap, remainingDepth) {
    var _a, _b, _c, _d;
    const n = node;
    const out = { id: node.id, name: node.name, type: node.type };
    if (n.visible === false) out.visible = false;
    const bb = n.absoluteBoundingBox;
    if (bb) out.absoluteBoundingBox = { x: bb.x, y: bb.y, width: bb.width, height: bb.height };
    if (n.layoutMode && n.layoutMode !== "NONE") {
      out.layoutMode = n.layoutMode;
      out.itemSpacing = n.itemSpacing;
      out.paddingLeft = n.paddingLeft;
      out.paddingRight = n.paddingRight;
      out.paddingTop = n.paddingTop;
      out.paddingBottom = n.paddingBottom;
      out.primaryAxisAlignItems = n.primaryAxisAlignItems;
      out.counterAxisAlignItems = n.counterAxisAlignItems;
      if (n.layoutWrap) out.layoutWrap = n.layoutWrap;
      if (typeof n.counterAxisSpacing === "number") out.counterAxisSpacing = n.counterAxisSpacing;
    }
    if (n.layoutPositioning === "ABSOLUTE") out.layoutPositioning = "ABSOLUTE";
    if (typeof n.layoutGrow === "number" && n.layoutGrow > 0) out.layoutGrow = n.layoutGrow;
    if (typeof n.layoutAlign === "string" && n.layoutAlign !== "INHERIT") {
      out.layoutAlign = n.layoutAlign;
    }
    if (n.layoutSizingHorizontal === "FILL" || n.layoutSizingHorizontal === "HUG") {
      out.layoutSizingHorizontal = n.layoutSizingHorizontal;
    }
    if (n.layoutSizingVertical === "FILL" || n.layoutSizingVertical === "HUG") {
      out.layoutSizingVertical = n.layoutSizingVertical;
    }
    const bv = n.boundVariables;
    if (Array.isArray(n.fills)) {
      const boundFills = bv == null ? void 0 : bv.fills;
      out.fills = n.fills.map(
        (p, i) => normalizePaint(p, varNameFor(Array.isArray(boundFills) ? boundFills[i] : void 0, varMap))
      );
    }
    if (Array.isArray(n.strokes)) {
      const boundStrokes = bv == null ? void 0 : bv.strokes;
      out.strokes = n.strokes.map(
        (p, i) => normalizePaint(p, varNameFor(Array.isArray(boundStrokes) ? boundStrokes[i] : void 0, varMap))
      );
    }
    if (typeof n.strokeWeight === "number") out.strokeWeight = n.strokeWeight;
    if (typeof n.strokeAlign === "string") out.strokeAlign = n.strokeAlign;
    if (typeof n.strokeTopWeight === "number") out.strokeTopWeight = n.strokeTopWeight;
    if (typeof n.strokeRightWeight === "number") out.strokeRightWeight = n.strokeRightWeight;
    if (typeof n.strokeBottomWeight === "number") out.strokeBottomWeight = n.strokeBottomWeight;
    if (typeof n.strokeLeftWeight === "number") out.strokeLeftWeight = n.strokeLeftWeight;
    if (Array.isArray(n.dashPattern) && n.dashPattern.length > 0) {
      out.dashPattern = n.dashPattern;
    }
    if (typeof n.cornerRadius === "number") {
      out.cornerRadius = n.cornerRadius;
      const radiusBinding = bv == null ? void 0 : bv.cornerRadius;
      const radiusVar = varNameFor(radiusBinding, varMap);
      if (radiusVar) out.cornerRadiusVar = radiusVar;
    } else if (typeof n.topLeftRadius === "number") {
      out.rectangleCornerRadii = [
        (_a = n.topLeftRadius) != null ? _a : 0,
        (_b = n.topRightRadius) != null ? _b : 0,
        (_c = n.bottomRightRadius) != null ? _c : 0,
        (_d = n.bottomLeftRadius) != null ? _d : 0
      ];
    }
    if (Array.isArray(n.effects)) out.effects = n.effects;
    if (typeof n.opacity === "number" && n.opacity < 1) out.opacity = n.opacity;
    if (n.clipsContent === true) out.clipsContent = true;
    if (typeof n.rotation === "number" && Math.abs(n.rotation) > 1e-4) {
      out.rotation = -(n.rotation * 180) / Math.PI;
    }
    if (typeof n.blendMode === "string" && n.blendMode !== "PASS_THROUGH" && n.blendMode !== "NORMAL") {
      out.blendMode = n.blendMode;
    }
    if (typeof n.cornerSmoothing === "number" && n.cornerSmoothing > 0) {
      out.cornerSmoothing = n.cornerSmoothing;
    }
    if (typeof n.textAutoResize === "string" && n.textAutoResize !== "NONE") {
      out.textAutoResize = n.textAutoResize;
    }
    const cons = n.constraints;
    if (cons && (cons.horizontal || cons.vertical)) {
      out.constraints = { horizontal: cons.horizontal, vertical: cons.vertical };
    }
    if (typeof n.minWidth === "number") out.minWidth = n.minWidth;
    if (typeof n.maxWidth === "number") out.maxWidth = n.maxWidth;
    if (typeof n.minHeight === "number") out.minHeight = n.minHeight;
    if (typeof n.maxHeight === "number") out.maxHeight = n.maxHeight;
    try {
      const vp = n.variantProperties;
      if (vp && typeof vp === "object") out.variantProperties = vp;
    } catch (e) {
    }
    if (n.isMask === true) {
      out.isMask = true;
      if (typeof n.maskType === "string") out.maskType = n.maskType;
    }
    if (Array.isArray(n.fillGeometry) && n.fillGeometry.length > 0 && (node.type === "VECTOR" || node.type === "BOOLEAN_OPERATION" || node.type === "STAR" || node.type === "POLYGON" || node.type === "LINE" || node.type === "ELLIPSE")) {
      out.fillGeometry = n.fillGeometry.map((g) => {
        const path = typeof g.path === "string" ? g.path : g.data;
        const item = {};
        if (typeof path === "string") item.path = path;
        if (typeof g.windingRule === "string") item.windingRule = g.windingRule;
        return item;
      });
    }
    if (node.type === "TEXT") {
      const t = node;
      out.characters = t.characters;
      const style = {};
      if (typeof t.fontSize === "number") style.fontSize = t.fontSize;
      if (t.fontName !== figma.mixed) {
        style.fontFamily = t.fontName.family;
        style.fontWeight = weightOf(t.fontName.style);
      }
      if (t.lineHeight !== figma.mixed) {
        if (t.lineHeight.unit === "PIXELS") {
          style.lineHeightPx = t.lineHeight.value;
        } else if (t.lineHeight.unit === "PERCENT" && typeof t.fontSize === "number") {
          style.lineHeightPx = t.fontSize * t.lineHeight.value / 100;
        }
      }
      if (t.letterSpacing !== figma.mixed && t.letterSpacing.unit === "PIXELS") {
        style.letterSpacing = t.letterSpacing.value;
      }
      style.textAlignHorizontal = t.textAlignHorizontal;
      if (t.textDecoration && t.textDecoration !== figma.mixed && t.textDecoration !== "NONE") {
        style.textDecoration = t.textDecoration;
      }
      out.style = style;
      try {
        const segments = t.getStyledTextSegments([
          "fontSize",
          "fontName",
          "fills",
          "lineHeight",
          "letterSpacing",
          "textDecoration"
        ]);
        if (segments.length > 1) {
          out.characterRuns = segments.map((seg) => {
            const runStyle = {};
            if (typeof seg.fontSize === "number") runStyle.fontSize = seg.fontSize;
            if (seg.fontName) {
              runStyle.fontFamily = seg.fontName.family;
              runStyle.fontWeight = weightOf(seg.fontName.style);
            }
            if (seg.lineHeight) {
              if (seg.lineHeight.unit === "PIXELS") {
                runStyle.lineHeightPx = seg.lineHeight.value;
              } else if (seg.lineHeight.unit === "PERCENT" && typeof seg.fontSize === "number") {
                runStyle.lineHeightPx = seg.fontSize * seg.lineHeight.value / 100;
              }
            }
            if (seg.letterSpacing && seg.letterSpacing.unit === "PIXELS") {
              runStyle.letterSpacing = seg.letterSpacing.value;
            }
            if (seg.textDecoration && seg.textDecoration !== "NONE") {
              runStyle.textDecoration = seg.textDecoration;
            }
            const runOut = {
              characters: seg.characters,
              style: runStyle
            };
            if (Array.isArray(seg.fills) && seg.fills.length > 0) {
              runOut.fills = seg.fills.map((p) => normalizePaint(p, void 0));
            }
            return runOut;
          });
        }
      } catch (e) {
      }
    }
    if (node.type === "INSTANCE") {
      try {
        const main = node.mainComponent;
        if (main) out.componentId = main.id;
      } catch (e) {
      }
      try {
        const props = node.componentProperties;
        if (props && typeof props === "object" && Object.keys(props).length > 0) {
          out.componentProperties = props;
        }
      } catch (e) {
      }
    }
    const reactions = serializeReactions(node);
    if (reactions) out.reactions = reactions;
    if ("children" in node) {
      const kids = node.children;
      if (kids.length > 0) {
        if (remainingDepth === 0) {
          out.childCount = kids.length;
        } else {
          const next = remainingDepth === void 0 ? void 0 : remainingDepth - 1;
          out.children = kids.map((kid) => {
            try {
              return serialize(kid, varMap, next);
            } catch (e) {
              return { id: kid.id, name: kid.name, type: kid.type };
            }
          });
        }
      }
    }
    return out;
  }
  var SCREEN_TYPES = ["FRAME", "COMPONENT", "INSTANCE"];
  function frameEntry(n) {
    const bb = n.absoluteBoundingBox;
    return {
      id: n.id,
      name: n.name,
      w: bb ? Math.round(bb.width) : 0,
      h: bb ? Math.round(bb.height) : 0
    };
  }
  function collectScreens(page) {
    const out = [];
    for (const child of page.children) {
      if (child.visible === false) continue;
      if (child.type === "SECTION" && Array.isArray(child.children)) {
        for (const inner of child.children) {
          if (inner.visible !== false && SCREEN_TYPES.indexOf(inner.type) !== -1) {
            out.push(frameEntry(inner));
          }
        }
      } else if (SCREEN_TYPES.indexOf(child.type) !== -1) {
        out.push(frameEntry(child));
      }
    }
    return out;
  }
  function pushInventory() {
    const pages = figma.root.children.map((page) => ({
      id: page.id,
      name: page.name,
      frames: collectScreens(page)
    }));
    figma.ui.postMessage({ type: "inventory", fileName: figma.root.name, pages });
  }
  function pushSelection() {
    const selection = figma.currentPage.selection;
    const node = selection.length > 0 ? selection[0] : null;
    figma.ui.postMessage({
      type: "selection",
      doc: node ? serialize(node) : null,
      fileName: figma.root.name,
      pageName: figma.currentPage.name,
      nodeName: node ? node.name : null,
      pluginVersion: PLUGIN_VERSION
    });
  }
  var VECTOR_TYPES = ["VECTOR", "BOOLEAN_OPERATION", "STAR", "LINE", "POLYGON"];
  var CONTAINER_TYPES = ["FRAME", "GROUP", "INSTANCE", "COMPONENT"];
  var MAX_ASSETS = 300;
  function hasImageFill(n) {
    return Array.isArray(n.fills) && n.fills.some((f) => f && f.type === "IMAGE" && f.visible !== false);
  }
  function firstImageHash(n) {
    if (!Array.isArray(n.fills)) return null;
    for (const f of n.fills) {
      if (f && f.type === "IMAGE" && f.visible !== false && typeof f.imageHash === "string") {
        return f.imageHash;
      }
    }
    return null;
  }
  function detectImageFormat(bytes) {
    if (bytes.length >= 4) {
      if (bytes[0] === 255 && bytes[1] === 216 && bytes[2] === 255) return "JPG";
      if (bytes[0] === 137 && bytes[1] === 80 && bytes[2] === 78 && bytes[3] === 71) return "PNG";
      if (bytes[0] === 71 && bytes[1] === 73 && bytes[2] === 70 && bytes[3] === 56) return "GIF";
      if (bytes.length >= 12 && bytes[0] === 82 && bytes[1] === 73 && bytes[2] === 70 && bytes[3] === 70 && bytes[8] === 87 && bytes[9] === 69 && bytes[10] === 66 && bytes[11] === 80) return "WEBP";
    }
    return "PNG";
  }
  function isIconSubtree(n) {
    let count = 0;
    let hasVector = false;
    let ok = true;
    function walk(x) {
      if (!ok) return;
      if (++count > 200) {
        ok = false;
        return;
      }
      if (x.type === "TEXT" || hasImageFill(x)) {
        ok = false;
        return;
      }
      if (VECTOR_TYPES.indexOf(x.type) !== -1) hasVector = true;
      if (Array.isArray(x.children)) for (const c of x.children) walk(c);
    }
    walk(n);
    return ok && hasVector;
  }
  function isVectorGroup(n) {
    if (n.type !== "GROUP") return false;
    let hasVector = false;
    let hasText = false;
    function walk(x) {
      if (hasText) return;
      if (x.type === "TEXT") {
        hasText = true;
        return;
      }
      if (VECTOR_TYPES.indexOf(x.type) !== -1) hasVector = true;
      if (Array.isArray(x.children)) for (const c of x.children) walk(c);
    }
    walk(n);
    return hasVector && !hasText;
  }
  function settingsFormat(n) {
    if (Array.isArray(n.exportSettings)) {
      for (const s of n.exportSettings) if (s && s.format === "SVG") return "SVG";
    }
    return "PNG";
  }
  var uploadAcks = /* @__PURE__ */ new Map();
  async function uploadBytes(bytes, ext, reqId, index) {
    const ackKey = `${reqId}-${index}`;
    const ack = new Promise((resolve, reject) => {
      uploadAcks.set(ackKey, (error) => error ? reject(new Error(error)) : resolve());
    });
    figma.ui.postMessage({
      type: "upload-asset",
      reqId,
      index,
      bytes,
      ext
    });
    await ack;
  }
  async function exportAndUpload(n, format, reqId, index) {
    const settings = format === "SVG" ? { format: "SVG" } : { format: "PNG", constraint: { type: "SCALE", value: 2 } };
    const bytes = await n.exportAsync(settings);
    await uploadBytes(bytes, format === "SVG" ? "svg" : "png", reqId, index);
    return { id: n.id, name: n.name, format, path: null };
  }
  async function extractRawImageAndUpload(n, reqId, index) {
    const hash = firstImageHash(n);
    if (!hash) return null;
    const image = figma.getImageByHash(hash);
    if (!image) return null;
    const bytes = await image.getBytesAsync();
    const format = detectImageFormat(bytes);
    const ext = format === "PNG" ? "png" : format === "JPG" ? "jpg" : format === "GIF" ? "gif" : "webp";
    await uploadBytes(bytes, ext, reqId, index);
    return { id: n.id, name: n.name, format, path: null };
  }
  function pickFormatForNode(n) {
    if (Array.isArray(n.exportSettings) && n.exportSettings.length > 0) {
      return settingsFormat(n);
    }
    if (hasImageFill(n)) return "PNG";
    return "SVG";
  }
  async function collectSpecific(ids, reqId, list, raw) {
    const assets = [];
    for (const id of ids) {
      if (assets.length >= MAX_ASSETS) break;
      const node = await figma.getNodeByIdAsync(id);
      if (!node || node.type === "PAGE" || node.type === "DOCUMENT") continue;
      const n = node;
      if (n.visible === false) continue;
      const format = pickFormatForNode(n);
      if (list) {
        assets.push({ id: n.id, name: n.name, format, path: null });
        continue;
      }
      if (raw && hasImageFill(n)) {
        const rawAsset = await extractRawImageAndUpload(n, reqId, assets.length);
        if (rawAsset) {
          assets.push(rawAsset);
          continue;
        }
      }
      assets.push(await exportAndUpload(n, format, reqId, assets.length));
    }
    return assets;
  }
  async function collectAssets(root, reqId, opts = {}) {
    const assets = [];
    async function visit(n, isRoot, ancestorExported, ancestorId) {
      if (assets.length >= MAX_ASSETS || n.visible === false) return;
      let format = null;
      if (!isRoot) {
        const hasExport = Array.isArray(n.exportSettings) && n.exportSettings.length > 0;
        if (hasExport) {
          format = settingsFormat(n);
        } else if (isVectorGroup(n)) {
          format = "SVG";
        } else if (CONTAINER_TYPES.indexOf(n.type) !== -1 && isIconSubtree(n)) {
          format = "SVG";
        } else if (VECTOR_TYPES.indexOf(n.type) !== -1 && !ancestorExported) {
          format = "SVG";
        } else if (hasImageFill(n)) {
          format = "PNG";
        }
      }
      let nextAncestorId = ancestorId;
      let exportedHere = false;
      if (format) {
        let asset;
        if (opts.list) {
          asset = { id: n.id, name: n.name, format, path: null };
        } else if (opts.raw && hasImageFill(n)) {
          const rawAsset = await extractRawImageAndUpload(n, reqId, assets.length);
          asset = rawAsset != null ? rawAsset : await exportAndUpload(n, format, reqId, assets.length);
        } else {
          asset = await exportAndUpload(n, format, reqId, assets.length);
        }
        if (ancestorId) asset.parentId = ancestorId;
        assets.push(asset);
        exportedHere = true;
        nextAncestorId = n.id;
      }
      if (Array.isArray(n.children)) {
        for (const c of n.children) {
          await visit(c, false, exportedHere || ancestorExported, nextAncestorId);
        }
      }
    }
    await visit(root, true, false, void 0);
    return assets;
  }
  function reply(message) {
    figma.ui.postMessage({ type: "plugin-reply", reply: message });
  }
  function errMsg(e) {
    return e instanceof Error ? e.message : String(e);
  }
  async function handleGetScreenshot(reqId, nodeId, scale, format) {
    var _a;
    try {
      const node = await figma.getNodeByIdAsync(nodeId);
      if (!node || node.type === "PAGE" || node.type === "DOCUMENT") {
        reply({ t: "screenshot", reqId, format: "", nodeName: null, error: "node not found" });
        return;
      }
      const f = format != null ? format : "PNG";
      const s = scale != null ? scale : 2;
      const settings = { format: f, constraint: { type: "SCALE", value: s } };
      const bytes = await node.exportAsync(settings);
      figma.ui.postMessage({
        type: "upload-screenshot",
        reqId,
        bytes,
        format: f,
        nodeName: (_a = node.name) != null ? _a : null
      });
    } catch (e) {
      reply({ t: "screenshot", reqId, format: "", nodeName: null, error: errMsg(e) });
    }
  }
  async function handleGetSearch(reqId, query, type) {
    try {
      let visit2 = function(n, page, parent) {
        var _a;
        if (matches.length >= MAX) return;
        if (n.visible === false) return;
        const nameOk = !q || ((_a = n.name) != null ? _a : "").toLowerCase().indexOf(q) !== -1;
        const typeOk = !t || n.type === t;
        if (nameOk && typeOk && n.type !== "PAGE" && n.type !== "DOCUMENT") {
          const bb = n.absoluteBoundingBox;
          const m = {
            id: n.id,
            name: n.name,
            type: n.type,
            page,
            w: bb ? Math.round(bb.width) : 0,
            h: bb ? Math.round(bb.height) : 0
          };
          if (parent) m.parentName = parent;
          matches.push(m);
        }
        if (Array.isArray(n.children) && n.type !== "INSTANCE") {
          for (const c of n.children) visit2(c, page, n.name);
        }
      };
      var visit = visit2;
      const q = (query != null ? query : "").trim().toLowerCase();
      const t = (type != null ? type : "").toUpperCase();
      const matches = [];
      const MAX = 50;
      for (const page of figma.root.children) {
        for (const child of page.children) visit2(child, page.name, "");
      }
      reply({ t: "search", reqId, matches, error: null });
    } catch (e) {
      reply({ t: "search", reqId, matches: [], error: errMsg(e) });
    }
  }
  async function handleGetComponents(reqId) {
    var _a, _b;
    try {
      let visit2 = function(n, page) {
        if (n.visible === false) return;
        if (n.type === "COMPONENT" || n.type === "COMPONENT_SET") {
          const bb = n.absoluteBoundingBox;
          const c = {
            id: n.id,
            name: n.name,
            page,
            w: bb ? Math.round(bb.width) : 0,
            h: bb ? Math.round(bb.height) : 0,
            instanceCount: 0
          };
          if (n.description) c.description = n.description;
          components.push(c);
        } else if (n.type === "INSTANCE") {
          instanceNodes.push({ node: n, page });
        }
        if (Array.isArray(n.children) && n.type !== "INSTANCE") {
          for (const c of n.children) visit2(c, page);
        }
      };
      var visit = visit2;
      const components = [];
      const instanceNodes = [];
      const instanceCount = /* @__PURE__ */ new Map();
      for (const page of figma.root.children) {
        for (const child of page.children) visit2(child, page.name);
      }
      const BATCH = 64;
      const resolved = [];
      for (let i = 0; i < instanceNodes.length; i += BATCH) {
        const slice = instanceNodes.slice(i, i + BATCH);
        const mains = await Promise.all(
          slice.map(
            ({ node: n }) => n.getMainComponentAsync().catch(() => null)
          )
        );
        for (let j = 0; j < slice.length; j++) {
          resolved.push({ n: slice[j].node, page: slice[j].page, main: mains[j] });
        }
      }
      const instances = [];
      for (const { n, page, main } of resolved) {
        if (!main) continue;
        instances.push({ id: n.id, name: n.name, componentId: main.id, page });
        instanceCount.set(main.id, ((_a = instanceCount.get(main.id)) != null ? _a : 0) + 1);
      }
      for (const c of components) c.instanceCount = (_b = instanceCount.get(c.id)) != null ? _b : 0;
      reply({ t: "components", reqId, components, instances, error: null });
    } catch (e) {
      reply({ t: "components", reqId, components: [], instances: [], error: errMsg(e) });
    }
  }
  var uploadOpQueue = Promise.resolve();
  function queueUploadOp(fn) {
    const next = uploadOpQueue.then(fn, fn);
    uploadOpQueue = next.catch(() => void 0);
    return next;
  }
  async function dispatchServerRequest(req) {
    if (!req || typeof req.t !== "string") return;
    if (req.t === "get-assets" || req.t === "get-screenshot" || req.t === "apply-design" || req.t === "apply-foundations" || req.t === "apply-motion") {
      await queueUploadOp(() => handleServerRequest(req));
      return;
    }
    await handleServerRequest(req);
  }
  async function handleServerRequest(req) {
    var _a;
    if (!req || typeof req.t !== "string") return;
    if (req.t === "get-node") {
      const node = await figma.getNodeByIdAsync(req.nodeId);
      if (!node || node.type === "PAGE" || node.type === "DOCUMENT") {
        reply({ t: "node", reqId: req.reqId, doc: null, nodeName: null });
        return;
      }
      const scene = node;
      const varMap = await buildVariableMap();
      reply({
        t: "node",
        reqId: req.reqId,
        doc: serialize(
          scene,
          varMap,
          typeof req.depth === "number" ? req.depth : void 0
        ),
        nodeName: scene.name
      });
      return;
    }
    if (req.t === "get-assets") {
      try {
        const list = req.list === true;
        const raw = req.raw === true;
        const ids = Array.isArray(req.ids) ? req.ids : void 0;
        if (ids && ids.length > 0) {
          const assets2 = await collectSpecific(ids, req.reqId, list, raw);
          reply({ t: "assets", reqId: req.reqId, assets: assets2, error: null });
          return;
        }
        const node = await figma.getNodeByIdAsync(req.nodeId);
        if (!node || node.type === "PAGE" || node.type === "DOCUMENT") {
          reply({ t: "assets", reqId: req.reqId, assets: [], error: "node not found" });
          return;
        }
        const assets = await collectAssets(node, req.reqId, { list, raw });
        reply({ t: "assets", reqId: req.reqId, assets, error: null });
      } catch (e) {
        reply({
          t: "assets",
          reqId: req.reqId,
          assets: [],
          error: e instanceof Error ? e.message : String(e)
        });
      }
      return;
    }
    if (req.t === "get-screenshot") {
      await handleGetScreenshot(req.reqId, req.nodeId, req.scale, req.format);
      return;
    }
    if (req.t === "get-search") {
      await handleGetSearch(req.reqId, req.query, req.type);
      return;
    }
    if (req.t === "get-components") {
      await handleGetComponents(req.reqId);
      return;
    }
    if (req.t === "apply-design") {
      try {
        const result = await applyDesign(
          req.plan,
          (phase, done, total, note) => reply({ t: "apply-progress", reqId: req.reqId, phase, done, total, note })
        );
        reply({ t: "applied", reqId: req.reqId, result, error: null });
      } catch (e) {
        reply({ t: "applied", reqId: req.reqId, result: null, error: errMsg(e) });
      }
      return;
    }
    if (req.t === "apply-foundations") {
      try {
        const result = await applyFoundations(req.plan, req.dryRun);
        reply({ t: "foundations", reqId: req.reqId, result, error: null });
      } catch (e) {
        reply({ t: "foundations", reqId: req.reqId, result: null, error: errMsg(e) });
      }
      return;
    }
    if (req.t === "apply-motion") {
      try {
        const idMap = new Map(Object.entries((_a = req.idMap) != null ? _a : {}));
        const result = await wireMotion(req.plan, idMap);
        reply({ t: "motion", reqId: req.reqId, result, error: null });
      } catch (e) {
        reply({ t: "motion", reqId: req.reqId, result: null, error: errMsg(e) });
      }
      return;
    }
  }
  figma.ui.onmessage = (message) => {
    var _a;
    if (!message || typeof message.type !== "string") return;
    switch (message.type) {
      case "resync":
        pushSelection();
        pushInventory();
        break;
      case "collapse":
        figma.ui.resize(DOT.w, DOT.h);
        break;
      case "expand":
        figma.ui.resize(PANEL.w, PANEL.h);
        break;
      case "paired":
        void figma.clientStorage.setAsync("plumb-paired", true);
        break;
      case "server-request":
        void dispatchServerRequest(message.req);
        break;
      case "upload-ack": {
        const key = `${message.reqId}-${message.index}`;
        const resolve = uploadAcks.get(key);
        if (resolve) {
          uploadAcks.delete(key);
          resolve((_a = message.error) != null ? _a : null);
        }
        break;
      }
    }
  };
  async function start() {
    await figma.loadAllPagesAsync();
    const wasPaired = await figma.clientStorage.getAsync("plumb-paired") === true;
    figma.ui.postMessage({ type: "init", autoPair: wasPaired });
    figma.on("selectionchange", pushSelection);
    let changeTimer = null;
    figma.on("documentchange", () => {
      invalidateVariableMapCache();
      if (changeTimer !== null) clearTimeout(changeTimer);
      changeTimer = setTimeout(() => {
        pushSelection();
        pushInventory();
      }, 400);
    });
    pushSelection();
    pushInventory();
  }
  void start();
})();
