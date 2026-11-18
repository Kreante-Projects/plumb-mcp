#!/usr/bin/env node

// src/assets/search.ts
var EXT_MIME = {
  png: "image/png",
  jpg: "image/jpeg",
  webp: "image/webp",
  gif: "image/gif",
  svg: "image/svg+xml"
};
var DEFAULTS = { limit: 16, timeoutMs: 1e4, perProvider: 24 };
async function httpJson(url, ctx, headers) {
  const res = await fetch(url, {
    signal: AbortSignal.timeout(ctx.timeoutMs),
    headers: { "user-agent": "plumb-mcp", accept: "application/json", ...headers }
  });
  if (!res.ok) throw new Error(`GET ${url} \u2192 ${res.status}`);
  return await res.json();
}
async function httpBytes(url, ctx, headers) {
  const res = await fetch(url, {
    signal: AbortSignal.timeout(ctx.timeoutMs),
    headers: { "user-agent": "plumb-mcp", ...headers }
  });
  if (!res.ok) throw new Error(`GET ${url} \u2192 ${res.status}`);
  const buf = new Uint8Array(await res.arrayBuffer());
  return { bytes: buf, contentType: res.headers.get("content-type") ?? "" };
}
function extFromContentType(ct, fallback) {
  const c = ct.toLowerCase();
  if (c.includes("svg")) return "svg";
  if (c.includes("png")) return "png";
  if (c.includes("webp")) return "webp";
  if (c.includes("gif")) return "gif";
  if (c.includes("jpeg") || c.includes("jpg")) return "jpg";
  return fallback;
}
async function fetchBitmap(url, ctx, fallbackExt = "jpg") {
  const { bytes, contentType } = await httpBytes(url, ctx);
  const ext = extFromContentType(contentType, fallbackExt);
  return { bytes, ext, mime: EXT_MIME[ext] };
}
async function fetchSvg(url, ctx) {
  const res = await fetch(url, {
    signal: AbortSignal.timeout(ctx.timeoutMs),
    headers: { "user-agent": "plumb-mcp", accept: "image/svg+xml" }
  });
  if (!res.ok) throw new Error(`GET ${url} \u2192 ${res.status}`);
  const svgText = await res.text();
  return {
    bytes: new TextEncoder().encode(svgText),
    ext: "svg",
    mime: EXT_MIME.svg,
    svgText
  };
}
var PROVIDER_RANK = {
  iconify: 0.9,
  unsplash: 0.9,
  pexels: 0.85,
  pixabay: 0.8,
  dicebear: 0.8,
  picsum: 0.4
};
var PREFIX_STYLE = {
  ph: ["line", "duotone", "outline"],
  lucide: ["line", "outline"],
  tabler: ["line", "outline"],
  "mdi-light": ["line", "outline"],
  mdi: ["filled", "flat"],
  "material-symbols": ["filled", "outline"],
  "material-symbols-light": ["outline", "line"],
  carbon: ["line", "outline"],
  solar: ["line", "duotone", "filled"],
  "fluent-emoji": ["3d"],
  "fluent-emoji-flat": ["flat"],
  noto: ["3d"],
  "twemoji": ["flat"],
  heroicons: ["outline", "filled", "line"],
  "heroicons-outline": ["outline", "line"],
  "heroicons-solid": ["filled"],
  bi: ["filled", "line"],
  fa6: ["filled"],
  "fa6-regular": ["outline", "line"]
};
function tokenize(s) {
  return s.toLowerCase().match(/[a-z0-9]+/g) ?? [];
}
function overlap(needles, haystack) {
  if (needles.length === 0) return 0;
  let hit = 0;
  for (const n of needles) if (haystack.has(n)) hit++;
  return hit / needles.length;
}
function scoreCandidate(c, spec) {
  const q = tokenize(spec.query);
  const hay = new Set(
    tokenize([c.title ?? "", (c.tags ?? []).join(" "), c.id].join(" "))
  );
  let s = 0.45 * overlap(q, hay);
  if (spec.kind) s += c.kind === spec.kind ? 0.2 : -0.15;
  if (spec.style?.length && c.style?.length) {
    const cs = new Set(c.style);
    let so = 0;
    for (const want of spec.style) if (cs.has(want)) so++;
    s += 0.15 * (so / spec.style.length);
  }
  if (spec.weight && c.weight) s += c.weight === spec.weight ? 0.08 : 0;
  if (spec.aspect && c.w && c.h) {
    const a = c.w / c.h;
    const d = Math.abs(a - spec.aspect) / spec.aspect;
    s += 0.1 * Math.max(0, 1 - d);
  }
  if (spec.minWidth && c.w && c.w < spec.minWidth) s -= 0.15;
  s += 0.12 * (PROVIDER_RANK[c.provider] ?? 0.5);
  return s;
}
function rankCandidates(cands, spec) {
  const scored = cands.map((c) => ({ ...c, score: scoreCandidate(c, spec) }));
  scored.sort((a, b) => (b.score ?? 0) - (a.score ?? 0));
  return scored;
}
function pickIconPack(ranked, spec) {
  const icons = ranked.filter((c) => c.provider === "iconify" && c.kind === "icon");
  if (icons.length === 0) return { candidates: ranked };
  const wantStyle = new Set(spec.style ?? []);
  const packWeight = /* @__PURE__ */ new Map();
  for (const c of icons) {
    const prefix = c.id.includes(":") ? c.id.slice(0, c.id.indexOf(":")) : c.provider;
    let w = (c.score ?? 0) + 0.15;
    const styles = PREFIX_STYLE[prefix];
    if (styles && wantStyle.size > 0) {
      const affinity = styles.filter((st) => wantStyle.has(st)).length;
      w += 0.25 * affinity;
    }
    packWeight.set(prefix, (packWeight.get(prefix) ?? 0) + w);
  }
  let best;
  let bestW = -Infinity;
  for (const [prefix, w] of packWeight) {
    if (w > bestW) {
      bestW = w;
      best = prefix;
    }
  }
  if (!best) return { candidates: ranked };
  const locked = icons.filter(
    (c) => (c.id.includes(":") ? c.id.slice(0, c.id.indexOf(":")) : c.provider) === best
  );
  return { prefix: best, candidates: locked };
}
function keyPresent(ctx, key) {
  return !!key && typeof ctx.env[key] === "string" && ctx.env[key].length > 0;
}
function stylesForPrefix(prefix) {
  return PREFIX_STYLE[prefix];
}
var iconifyProvider = {
  id: "iconify",
  kinds: ["icon"],
  keyless: true,
  async search(spec, ctx) {
    if (spec.kind && spec.kind !== "icon") return [];
    const q = encodeURIComponent(spec.query);
    const data = await httpJson(
      `https://api.iconify.design/search?query=${q}&limit=${ctx.perProvider}`,
      ctx
    );
    const icons = data.icons ?? [];
    const color = spec.palette?.[0];
    return icons.map((full) => {
      const idx = full.indexOf(":");
      const prefix = idx >= 0 ? full.slice(0, idx) : full;
      const name = idx >= 0 ? full.slice(idx + 1) : full;
      const svgUrl = `https://api.iconify.design/${prefix}/${name}.svg` + (color ? `?color=${encodeURIComponent(color)}` : "");
      return {
        id: full,
        provider: "iconify",
        kind: "icon",
        title: name.replace(/-/g, " "),
        tags: [prefix, ...name.split("-")],
        style: stylesForPrefix(prefix),
        license: "Iconify \u2014 per-set (mostly permissive)",
        fetch: (c) => fetchSvg(svgUrl, c)
      };
    });
  }
};
var DICEBEAR_STYLES = [
  "lorelei",
  "thumbs",
  "notionists",
  "avataaars",
  "bottts",
  "fun-emoji",
  "identicon",
  "initials",
  "shapes"
];
var dicebearProvider = {
  id: "dicebear",
  kinds: ["avatar"],
  keyless: true,
  async search(spec, ctx) {
    if (spec.kind && spec.kind !== "avatar") return [];
    const seed = encodeURIComponent(spec.seed ?? spec.query ?? "plumb");
    const qs = tokenize(spec.query);
    const ordered = [...DICEBEAR_STYLES].sort((a, b) => {
      const sa = qs.includes(a) ? 1 : 0;
      const sb = qs.includes(b) ? 1 : 0;
      return sb - sa;
    });
    return ordered.slice(0, Math.min(ctx.perProvider, ordered.length)).map(
      (style) => {
        const url = `https://api.dicebear.com/9.x/${style}/svg?seed=${seed}`;
        return {
          id: `dicebear:${style}:${seed}`,
          provider: "dicebear",
          kind: "avatar",
          title: `${style} avatar`,
          tags: ["avatar", style, ...qs],
          license: "DiceBear \u2014 per-style (mostly CC0 / free)",
          fetch: (c) => fetchSvg(url, c)
        };
      }
    );
  }
};
var picsumProvider = {
  id: "picsum",
  kinds: ["photo"],
  keyless: true,
  async search(spec, ctx) {
    if (spec.kind && spec.kind !== "photo") return [];
    void ctx;
    const w = spec.w ?? spec.minWidth ?? 1200;
    const h = spec.h ?? (spec.aspect ? Math.round(w / spec.aspect) : Math.round(w * 0.66));
    const seed = encodeURIComponent(spec.seed ?? spec.query ?? "plumb");
    const url = `https://picsum.photos/seed/${seed}/${w}/${h}`;
    return [
      {
        id: `picsum:${seed}:${w}x${h}`,
        provider: "picsum",
        kind: "photo",
        title: spec.query || "photo",
        tags: ["photo", "placeholder"],
        w,
        h,
        url,
        scaleMode: "fill",
        license: "Lorem Picsum \u2014 Unsplash License (free)",
        fetch: (c) => fetchBitmap(url, c, "jpg")
      }
    ];
  }
};
var unsplashProvider = {
  id: "unsplash",
  kinds: ["photo"],
  keyless: false,
  envKey: "UNSPLASH_ACCESS_KEY",
  available: (ctx) => keyPresent(ctx, "UNSPLASH_ACCESS_KEY"),
  async search(spec, ctx) {
    if (spec.kind && spec.kind !== "photo") return [];
    const key = ctx.env.UNSPLASH_ACCESS_KEY;
    const q = encodeURIComponent(spec.query);
    const data = await httpJson(
      `https://api.unsplash.com/search/photos?query=${q}&per_page=${ctx.perProvider}`,
      ctx,
      { authorization: `Client-ID ${key}` }
    );
    return (data.results ?? []).map((r) => {
      const url = r.urls?.regular ?? r.urls?.full ?? r.urls?.small ?? "";
      return {
        id: `unsplash:${r.id}`,
        provider: "unsplash",
        kind: "photo",
        title: r.alt_description ?? r.description ?? spec.query,
        tags: tokenize(`${r.alt_description ?? ""} ${r.description ?? ""}`),
        w: r.width,
        h: r.height,
        url,
        thumbUrl: r.urls?.small,
        scaleMode: "fill",
        license: "Unsplash License",
        attribution: r.user?.name ? `Photo by ${r.user.name} on Unsplash` : void 0,
        fetch: (c) => fetchBitmap(url, c, "jpg")
      };
    });
  }
};
var pexelsProvider = {
  id: "pexels",
  kinds: ["photo"],
  keyless: false,
  envKey: "PEXELS_API_KEY",
  available: (ctx) => keyPresent(ctx, "PEXELS_API_KEY"),
  async search(spec, ctx) {
    if (spec.kind && spec.kind !== "photo") return [];
    const key = ctx.env.PEXELS_API_KEY;
    const q = encodeURIComponent(spec.query);
    const data = await httpJson(
      `https://api.pexels.com/v1/search?query=${q}&per_page=${ctx.perProvider}`,
      ctx,
      { authorization: key }
    );
    return (data.photos ?? []).map((p) => {
      const url = p.src?.large ?? p.src?.large2x ?? p.src?.medium ?? p.src?.original ?? "";
      return {
        id: `pexels:${p.id}`,
        provider: "pexels",
        kind: "photo",
        title: p.alt ?? spec.query,
        tags: tokenize(p.alt ?? ""),
        w: p.width,
        h: p.height,
        url,
        scaleMode: "fill",
        license: "Pexels License",
        attribution: p.photographer ? `Photo by ${p.photographer} on Pexels` : void 0,
        fetch: (c) => fetchBitmap(url, c, "jpg")
      };
    });
  }
};
var pixabayProvider = {
  id: "pixabay",
  kinds: ["photo", "illustration"],
  keyless: false,
  envKey: "PIXABAY_API_KEY",
  available: (ctx) => keyPresent(ctx, "PIXABAY_API_KEY"),
  async search(spec, ctx) {
    if (spec.kind && spec.kind !== "photo" && spec.kind !== "illustration") return [];
    const key = ctx.env.PIXABAY_API_KEY;
    const q = encodeURIComponent(spec.query);
    const imageType = spec.kind === "illustration" ? "illustration" : "photo";
    const data = await httpJson(
      `https://pixabay.com/api/?key=${encodeURIComponent(key)}&q=${q}&image_type=${imageType}&per_page=${Math.max(3, ctx.perProvider)}`,
      ctx
    );
    return (data.hits ?? []).map((hh) => {
      const url = hh.largeImageURL ?? hh.webformatURL ?? "";
      return {
        id: `pixabay:${hh.id}`,
        provider: "pixabay",
        kind: imageType === "illustration" ? "illustration" : "photo",
        title: hh.tags ?? spec.query,
        tags: tokenize(hh.tags ?? ""),
        w: hh.imageWidth,
        h: hh.imageHeight,
        url,
        scaleMode: "fill",
        license: "Pixabay Content License",
        attribution: hh.user ? `Image by ${hh.user} on Pixabay` : void 0,
        fetch: (c) => fetchBitmap(url, c, "jpg")
      };
    });
  }
};
var BUILTIN_PROVIDERS = [
  iconifyProvider,
  dicebearProvider,
  unsplashProvider,
  pexelsProvider,
  pixabayProvider,
  picsumProvider
];
var registry = new Map(
  BUILTIN_PROVIDERS.map((p) => [p.id, p])
);
function registerAssetProvider(provider) {
  registry.set(provider.id, provider);
}
function listAssetProviders() {
  return [...registry.values()];
}
var _bundledReady = null;
function ensureBundledProviders() {
  if (!_bundledReady) {
    _bundledReady = import("./register-KZJWPGYL.js").then((m) => m.registerBundledAssetProviders()).catch(() => {
    });
  }
  return _bundledReady;
}
function isAvailable(p, ctx) {
  if (p.available) return p.available(ctx);
  return p.keyless || keyPresent(ctx, p.envKey);
}
function makeCtx(opts = {}) {
  return {
    env: opts.env ?? process.env,
    timeoutMs: opts.timeoutMs ?? DEFAULTS.timeoutMs,
    perProvider: opts.perProvider ?? DEFAULTS.perProvider
  };
}
async function searchAssets(spec, opts = {}) {
  await ensureBundledProviders();
  const ctx = makeCtx(opts);
  const limit = opts.limit ?? DEFAULTS.limit;
  const providersRun = [];
  const providersDropped = [];
  const eligible = listAssetProviders().filter((p) => {
    if (spec.provider && p.id !== spec.provider) {
      return false;
    }
    if (spec.kind && !p.kinds.includes(spec.kind)) {
      providersDropped.push({ id: p.id, reason: `does not serve kind "${spec.kind}"` });
      return false;
    }
    if (!isAvailable(p, ctx)) {
      providersDropped.push({
        id: p.id,
        reason: p.envKey ? `missing env ${p.envKey}` : "unavailable"
      });
      return false;
    }
    return true;
  });
  const settled = await Promise.allSettled(
    eligible.map(async (p) => ({ id: p.id, hits: await p.search(spec, ctx) }))
  );
  let all = [];
  settled.forEach((r, i) => {
    const p = eligible[i];
    if (r.status === "fulfilled") {
      providersRun.push(p.id);
      all = all.concat(r.value.hits);
    } else {
      providersDropped.push({
        id: p.id,
        reason: r.reason instanceof Error ? r.reason.message : String(r.reason)
      });
    }
  });
  const hasRealPhoto = all.some((c) => c.kind === "photo" && c.provider !== "picsum");
  if (hasRealPhoto) all = all.filter((c) => c.provider !== "picsum");
  let ranked = rankCandidates(all, spec);
  let iconPack;
  const wantsIcons = spec.kind === "icon" || ranked.some((c) => c.kind === "icon");
  if (wantsIcons) {
    const nonIcons = ranked.filter((c) => c.kind !== "icon");
    const pack = pickIconPack(ranked, spec);
    iconPack = pack.prefix;
    ranked = [...pack.candidates, ...nonIcons];
  }
  return {
    candidates: ranked.slice(0, limit),
    providersRun,
    providersDropped,
    iconPack
  };
}
var PLACEHOLDER_QUERIES = {
  icon: { query: "image", kind: "icon" },
  photo: { query: "abstract", kind: "photo" },
  avatar: { query: "user", kind: "avatar" },
  illustration: { query: "abstract", kind: "illustration" }
};
async function resolveAsset(spec, opts = {}) {
  await ensureBundledProviders();
  const ctx = makeCtx(opts);
  const attempt = async (candidates) => {
    for (const c of candidates) {
      try {
        const got = await c.fetch(ctx);
        const inlineSvg = got.ext === "svg" && got.svgText && got.svgText.length <= 8 * 1024 ? got.svgText : void 0;
        return {
          kind: c.kind,
          w: c.w,
          h: c.h,
          url: c.url,
          scaleMode: c.scaleMode,
          inlineSvg,
          bytes: got.bytes,
          ext: got.ext,
          mime: got.mime,
          provider: c.provider,
          title: c.title,
          attribution: c.attribution,
          license: c.license
        };
      } catch {
      }
    }
    return null;
  };
  try {
    const { candidates } = await searchAssets(spec, { ...opts, limit: opts.limit ?? 8 });
    const hit = await attempt(candidates);
    if (hit) return hit;
  } catch {
  }
  const kind = spec.kind ?? "photo";
  const phSpec = {
    ...PLACEHOLDER_QUERIES[kind] ?? { query: spec.query || "placeholder", kind: "photo" },
    seed: spec.seed ?? spec.query,
    palette: spec.palette,
    w: spec.w,
    h: spec.h,
    aspect: spec.aspect
  };
  try {
    const { candidates } = await searchAssets(phSpec, { ...opts, limit: 4 });
    const hit = await attempt(candidates);
    if (hit) return { ...hit, placeholder: true };
  } catch {
  }
  return { kind, placeholder: true };
}

export {
  registerAssetProvider,
  searchAssets,
  resolveAsset
};
