#!/usr/bin/env node
import {
  registerAssetProvider
} from "./chunk-J4ZHAR5B.js";

// src/assets/types.ts
var LICENSES = {
  iconify: {
    id: "iconify",
    name: "Open-source icon (varies by set, see icon-sets.iconify.design)",
    url: "https://iconify.design/license/",
    requiresAttribution: false,
    commercial: true
  },
  unsplash: {
    id: "unsplash",
    name: "Unsplash License",
    url: "https://unsplash.com/license",
    requiresAttribution: false,
    attributionTemplate: "Photo by {author} on Unsplash",
    commercial: true
  },
  pexels: {
    id: "pexels",
    name: "Pexels License",
    url: "https://www.pexels.com/license/",
    requiresAttribution: false,
    attributionTemplate: "Photo by {author} on Pexels",
    commercial: true
  },
  pixabay: {
    id: "pixabay",
    name: "Pixabay Content License",
    url: "https://pixabay.com/service/license-summary/",
    requiresAttribution: false,
    attributionTemplate: "Image by {author} on Pixabay",
    commercial: true
  },
  picsum: {
    id: "picsum",
    name: "Lorem Picsum (Unsplash-sourced placeholder photos)",
    url: "https://picsum.photos/",
    requiresAttribution: false,
    commercial: true
  },
  dicebear: {
    id: "dicebear",
    name: "DiceBear Avatars (CC0 / MIT per style, see dicebear.com/licenses)",
    url: "https://www.dicebear.com/licenses/",
    requiresAttribution: false,
    commercial: true
  },
  googleFonts: {
    id: "google-fonts",
    name: "Open font license (OFL/Apache-2.0, varies per family)",
    url: "https://fonts.google.com/attribution",
    requiresAttribution: false,
    commercial: true
  },
  /** Placeholder art authored fresh for Plumb (illustrations/mockups/patterns
   *  bundled providers) — never a traced/derived copy of a third-party set,
   *  so it carries no third-party license obligation. */
  bundled: {
    id: "plumb-bundled",
    name: "CC0 \u2014 bundled with Plumb",
    requiresAttribution: false,
    commercial: true
  }
};

// src/assets/providers/illustrations.ts
var KIND = "illustration";
var DEFAULT_PALETTE = ["#F2C9A0", "#2D3436", "#F1F2F6"];
var CHARACTERS = [
  {
    slug: "standing",
    title: "Standing person",
    tags: ["person", "standing", "hero", "team", "profile"],
    svg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 220"><rect width="160" height="220" fill="%BG%"/><circle cx="80" cy="52" r="26" fill="%SKIN%"/><path d="M50 210V128a30 30 0 0160-4v86z" fill="%CLOTHES%"/><rect x="42" y="118" width="16" height="60" rx="8" fill="%SKIN%"/><rect x="102" y="118" width="16" height="60" rx="8" fill="%SKIN%"/></svg>`
  },
  {
    slug: "waving",
    title: "Waving / greeting",
    tags: ["person", "wave", "greeting", "hello", "welcome"],
    svg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 220"><rect width="160" height="220" fill="%BG%"/><circle cx="76" cy="52" r="26" fill="%SKIN%"/><path d="M48 210V130a28 28 0 0156-3v83z" fill="%CLOTHES%"/><rect x="40" y="120" width="16" height="58" rx="8" fill="%SKIN%"/><path d="M108 118c14-6 26-24 22-40" fill="none" stroke="%SKIN%" stroke-width="16" stroke-linecap="round"/></svg>`
  },
  {
    slug: "sitting",
    title: "Sitting person",
    tags: ["person", "sitting", "chair", "relaxed", "desk"],
    svg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 220"><rect width="160" height="220" fill="%BG%"/><circle cx="80" cy="60" r="24" fill="%SKIN%"/><path d="M52 190v-50a28 28 0 0156 0v50z" fill="%CLOTHES%"/><rect x="52" y="185" width="56" height="16" rx="8" fill="%CLOTHES%"/><rect x="40" y="196" width="80" height="10" rx="5" fill="%SKIN%" opacity="0.4"/></svg>`
  },
  {
    slug: "presenting",
    title: "Presenting / pointing",
    tags: ["person", "presenting", "pointing", "teaching", "product"],
    svg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 220"><rect width="160" height="220" fill="%BG%"/><circle cx="70" cy="52" r="26" fill="%SKIN%"/><path d="M42 210V128a30 30 0 0158-6v88z" fill="%CLOTHES%"/><path d="M96 128c22-4 40 6 48 26" fill="none" stroke="%SKIN%" stroke-width="16" stroke-linecap="round"/><rect x="32" y="118" width="16" height="60" rx="8" fill="%SKIN%"/></svg>`
  },
  {
    slug: "thinking",
    title: "Thinking / idea",
    tags: ["person", "thinking", "idea", "planning", "strategy"],
    svg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 220"><rect width="160" height="220" fill="%BG%"/><circle cx="80" cy="54" r="26" fill="%SKIN%"/><circle cx="118" cy="24" r="6" fill="%CLOTHES%"/><circle cx="130" cy="10" r="4" fill="%CLOTHES%"/><path d="M52 210V128a28 28 0 0156 0v82z" fill="%CLOTHES%"/><rect x="98" y="70" width="14" height="46" rx="7" fill="%SKIN%" transform="rotate(20 105 93)"/></svg>`
  },
  {
    slug: "celebrating",
    title: "Celebrating / success",
    tags: ["person", "celebrating", "success", "excited", "cheer"],
    svg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 160 220"><rect width="160" height="220" fill="%BG%"/><circle cx="80" cy="54" r="26" fill="%SKIN%"/><path d="M52 210V132a28 28 0 0156 0v78z" fill="%CLOTHES%"/><path d="M52 128c-8-18-4-40 8-50" fill="none" stroke="%SKIN%" stroke-width="16" stroke-linecap="round"/><path d="M108 128c8-18 4-40-8-50" fill="none" stroke="%SKIN%" stroke-width="16" stroke-linecap="round"/></svg>`
  }
];
function applyPalette(svg, palette) {
  return svg.replaceAll("%SKIN%", palette[0]).replaceAll("%CLOTHES%", palette[1]).replaceAll("%BG%", palette[2]);
}
function resolvePalette(spec) {
  const p = spec.palette;
  return [p?.[0] ?? DEFAULT_PALETTE[0], p?.[1] ?? DEFAULT_PALETTE[1], p?.[2] ?? DEFAULT_PALETTE[2]];
}
function createIllustrationsProvider() {
  return {
    id: "illustrations",
    kinds: [KIND],
    license: LICENSES.bundled,
    keyless: true,
    async search(spec) {
      const q = spec.query.trim().toLowerCase();
      const palette = resolvePalette(spec);
      const hits = q ? CHARACTERS.filter((c) => c.slug.includes(q) || c.tags.some((t) => t.includes(q) || q.includes(t))) : CHARACTERS;
      const pool = hits.length ? hits : CHARACTERS;
      return pool.map((c, i) => {
        const raw = { slug: c.slug, palette };
        return {
          id: c.slug,
          provider: "illustrations",
          kind: KIND,
          title: c.title,
          tags: c.tags,
          license: LICENSES.bundled,
          score: pool.length - i,
          raw
        };
      });
    },
    async fetch(candidate) {
      const raw = candidate.raw;
      const character = CHARACTERS.find((c) => c.slug === (raw?.slug ?? candidate.id));
      if (!character) throw new Error(`illustrations: unknown character "${candidate.id}"`);
      const svg = applyPalette(character.svg, raw?.palette ?? DEFAULT_PALETTE);
      return {
        svg,
        mime: "image/svg+xml",
        ext: "svg",
        width: 160,
        height: 220,
        kind: KIND,
        license: candidate.license
      };
    }
  };
}

// src/assets/providers/undraw.ts
var KIND2 = "illustration";
var DEFAULT_PALETTE2 = ["#6C5CE7", "#2D3436", "#F1F2F6"];
var SCENES = [
  {
    slug: "empty-state",
    title: "Empty state",
    tags: ["empty", "empty-state", "search", "no-results"],
    svg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 240 200"><rect width="240" height="200" fill="%BG%"/><circle cx="105" cy="95" r="48" fill="none" stroke="%ACCENT%" stroke-width="10"/><rect x="138" y="128" width="16" height="52" rx="8" transform="rotate(45 138 128)" fill="%DARK%"/><circle cx="105" cy="95" r="20" fill="%ACCENT%" opacity="0.25"/></svg>`
  },
  {
    slug: "not-found",
    title: "Page not found",
    tags: ["404", "error", "not-found", "lost"],
    svg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 240 200"><rect width="240" height="200" fill="%BG%"/><text x="120" y="120" font-family="sans-serif" font-size="72" font-weight="700" text-anchor="middle" fill="%ACCENT%">404</text><rect x="60" y="140" width="120" height="8" rx="4" fill="%DARK%" opacity="0.35"/></svg>`
  },
  {
    slug: "success",
    title: "Success / done",
    tags: ["success", "done", "complete", "checkmark"],
    svg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 240 200"><rect width="240" height="200" fill="%BG%"/><circle cx="120" cy="100" r="55" fill="%ACCENT%" opacity="0.15"/><circle cx="120" cy="100" r="38" fill="%ACCENT%"/><path d="M102 100l12 13 26-28" fill="none" stroke="%BG%" stroke-width="8" stroke-linecap="round" stroke-linejoin="round"/></svg>`
  },
  {
    slug: "onboarding-rocket",
    title: "Onboarding / launch",
    tags: ["onboarding", "launch", "start", "rocket", "welcome"],
    svg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 240 200"><rect width="240" height="200" fill="%BG%"/><path d="M120 40c22 18 26 60 14 92l-28 0c-12-32-8-74 14-92z" fill="%ACCENT%"/><circle cx="120" cy="82" r="10" fill="%BG%"/><path d="M96 118l-18 30 26-10z" fill="%DARK%"/><path d="M144 118l18 30-26-10z" fill="%DARK%"/><path d="M112 132l8 34 8-34z" fill="%ACCENT%" opacity="0.55"/></svg>`
  },
  {
    slug: "security",
    title: "Security / privacy",
    tags: ["security", "privacy", "shield", "protection"],
    svg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 240 200"><rect width="240" height="200" fill="%BG%"/><path d="M120 36l52 20v40c0 40-24 66-52 78-28-12-52-38-52-78V56z" fill="%ACCENT%"/><path d="M100 100l14 15 28-30" fill="none" stroke="%BG%" stroke-width="9" stroke-linecap="round" stroke-linejoin="round"/></svg>`
  },
  {
    slug: "analytics",
    title: "Analytics / data",
    tags: ["analytics", "data", "chart", "dashboard", "stats"],
    svg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 240 200"><rect width="240" height="200" fill="%BG%"/><rect x="50" y="120" width="26" height="50" rx="4" fill="%DARK%" opacity="0.4"/><rect x="90" y="90" width="26" height="80" rx="4" fill="%ACCENT%"/><rect x="130" y="60" width="26" height="110" rx="4" fill="%ACCENT%" opacity="0.7"/><rect x="170" y="105" width="26" height="65" rx="4" fill="%DARK%" opacity="0.4"/></svg>`
  },
  {
    slug: "upload",
    title: "Upload / cloud",
    tags: ["upload", "cloud", "sync", "file"],
    svg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 240 200"><rect width="240" height="200" fill="%BG%"/><path d="M80 130a30 30 0 010-60 40 40 0 0177 6 26 26 0 01-3 54H80z" fill="%ACCENT%" opacity="0.9"/><path d="M120 150V96m0 0l-16 16m16-16l16 16" fill="none" stroke="%BG%" stroke-width="8" stroke-linecap="round" stroke-linejoin="round"/></svg>`
  },
  {
    slug: "chat",
    title: "Chat / messaging",
    tags: ["chat", "messaging", "conversation", "support"],
    svg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 240 200"><rect width="240" height="200" fill="%BG%"/><rect x="55" y="60" width="130" height="80" rx="16" fill="%ACCENT%"/><path d="M75 140l0 24 26-24z" fill="%ACCENT%"/><circle cx="95" cy="100" r="7" fill="%BG%"/><circle cx="120" cy="100" r="7" fill="%BG%"/><circle cx="145" cy="100" r="7" fill="%BG%"/></svg>`
  }
];
function applyPalette2(svg, palette) {
  return svg.replaceAll("%ACCENT%", palette[0]).replaceAll("%DARK%", palette[1]).replaceAll("%BG%", palette[2]);
}
function resolvePalette2(spec) {
  const p = spec.palette;
  return [p?.[0] ?? DEFAULT_PALETTE2[0], p?.[1] ?? DEFAULT_PALETTE2[1], p?.[2] ?? DEFAULT_PALETTE2[2]];
}
function createUndrawProvider() {
  return {
    id: "undraw",
    kinds: [KIND2],
    license: LICENSES.bundled,
    keyless: true,
    async search(spec) {
      const q = spec.query.trim().toLowerCase();
      const palette = resolvePalette2(spec);
      const hits = q ? SCENES.filter((s) => s.slug.includes(q) || s.tags.some((t) => t.includes(q) || q.includes(t))) : SCENES;
      const pool = hits.length ? hits : SCENES;
      return pool.map((s, i) => {
        const raw = { slug: s.slug, palette };
        return {
          id: s.slug,
          provider: "undraw",
          kind: KIND2,
          title: s.title,
          tags: s.tags,
          license: LICENSES.bundled,
          score: pool.length - i,
          raw
        };
      });
    },
    async fetch(candidate) {
      const raw = candidate.raw;
      const scene = SCENES.find((s) => s.slug === (raw?.slug ?? candidate.id));
      if (!scene) throw new Error(`undraw: unknown scene "${candidate.id}"`);
      const svg = applyPalette2(scene.svg, raw?.palette ?? DEFAULT_PALETTE2);
      return {
        svg,
        mime: "image/svg+xml",
        ext: "svg",
        width: 240,
        height: 200,
        kind: KIND2,
        license: candidate.license
      };
    }
  };
}

// src/assets/providers/mockups.ts
var KIND3 = "mockup";
var MOCKUP_RECIPES = [
  {
    id: "phone",
    label: "Phone",
    device: "phone",
    canvasW: 300,
    canvasH: 610,
    contentRect: { x: 20, y: 20, w: 260, h: 570 },
    frameSvg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 300 610"><rect x="4" y="4" width="292" height="602" rx="44" fill="#1a1a1a"/><rect id="plumb-content-slot" x="20" y="20" width="260" height="570" rx="30" fill="#ffffff"/><rect x="110" y="14" width="80" height="14" rx="7" fill="#1a1a1a"/></svg>`
  },
  {
    id: "tablet",
    label: "Tablet",
    device: "tablet",
    canvasW: 520,
    canvasH: 700,
    contentRect: { x: 24, y: 24, w: 472, h: 652 },
    frameSvg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 520 700"><rect x="4" y="4" width="512" height="692" rx="28" fill="#1a1a1a"/><rect id="plumb-content-slot" x="24" y="24" width="472" height="652" rx="10" fill="#ffffff"/><circle cx="260" cy="14" r="4" fill="#3a3a3a"/></svg>`
  },
  {
    id: "laptop",
    label: "Laptop",
    device: "laptop",
    canvasW: 720,
    canvasH: 470,
    contentRect: { x: 60, y: 24, w: 600, h: 375 },
    frameSvg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 720 470"><rect x="40" y="4" width="640" height="410" rx="14" fill="#2b2b2b"/><rect id="plumb-content-slot" x="60" y="24" width="600" height="375" fill="#ffffff"/><path d="M0 440h720l-30 26H30z" fill="#3a3a3a"/><rect x="300" y="440" width="120" height="8" rx="4" fill="#1a1a1a"/></svg>`
  },
  {
    id: "browser",
    label: "Browser window",
    device: "browser",
    canvasW: 720,
    canvasH: 480,
    contentRect: { x: 0, y: 44, w: 720, h: 436 },
    frameSvg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 720 480"><rect width="720" height="480" rx="12" fill="#e6e8eb"/><rect width="720" height="44" rx="12" fill="#d4d7db"/><rect y="20" width="720" height="24" fill="#d4d7db"/><circle cx="24" cy="22" r="6" fill="#ff5f57"/><circle cx="46" cy="22" r="6" fill="#febc2e"/><circle cx="68" cy="22" r="6" fill="#28c840"/><rect x="100" y="12" width="520" height="20" rx="10" fill="#ffffff"/><rect id="plumb-content-slot" x="0" y="44" width="720" height="436" fill="#ffffff"/></svg>`
  },
  {
    id: "watch",
    label: "Smartwatch",
    device: "watch",
    canvasW: 260,
    canvasH: 300,
    contentRect: { x: 30, y: 30, w: 200, h: 200 },
    frameSvg: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 260 300"><rect x="90" y="0" width="80" height="40" rx="10" fill="#1a1a1a"/><rect x="90" y="260" width="80" height="40" rx="10" fill="#1a1a1a"/><rect x="10" y="20" width="240" height="260" rx="56" fill="#1a1a1a"/><rect id="plumb-content-slot" x="30" y="40" width="200" height="220" rx="40" fill="#ffffff"/></svg>`
  }
];
function getMockupRecipe(id) {
  return MOCKUP_RECIPES.find((m) => m.id === id);
}
function matchDevice(recipe, query) {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  const aliases = {
    phone: ["phone", "iphone", "mobile", "smartphone", "android"],
    tablet: ["tablet", "ipad"],
    laptop: ["laptop", "macbook", "notebook", "computer"],
    browser: ["browser", "window", "web", "desktop", "chrome", "safari"],
    watch: ["watch", "smartwatch", "apple watch", "wearable"]
  };
  return (aliases[recipe.device] ?? [recipe.device]).some((a) => a.includes(q) || q.includes(a));
}
function createMockupsProvider() {
  return {
    id: "mockups",
    kinds: [KIND3],
    license: LICENSES.bundled,
    keyless: true,
    async search(spec) {
      const hits = MOCKUP_RECIPES.filter((m) => matchDevice(m, spec.query));
      const pool = hits.length ? hits : MOCKUP_RECIPES;
      return pool.map((m, i) => ({
        id: m.id,
        provider: "mockups",
        kind: KIND3,
        title: m.label,
        tags: [m.device],
        width: m.canvasW,
        height: m.canvasH,
        license: LICENSES.bundled,
        score: pool.length - i,
        raw: { id: m.id }
      }));
    },
    async fetch(candidate) {
      const recipe = getMockupRecipe(candidate.id);
      if (!recipe) throw new Error(`mockups: unknown recipe "${candidate.id}"`);
      return {
        svg: recipe.frameSvg,
        mime: "image/svg+xml",
        ext: "svg",
        width: recipe.canvasW,
        height: recipe.canvasH,
        kind: KIND3,
        license: candidate.license
      };
    }
  };
}

// src/assets/providers/patterns.ts
var KIND4 = "pattern";
var DEFAULT_FG = "#000000";
var DEFAULT_BG_ALPHA = "0.06";
var PATTERNS = [
  {
    slug: "dots",
    title: "Polka dots",
    tags: ["dots", "polka", "spots"],
    tile: 20,
    body: `<circle cx="10" cy="10" r="2.5" fill="%FG%"/>`
  },
  {
    slug: "grid",
    title: "Grid",
    tags: ["grid", "lines", "graph"],
    tile: 24,
    body: `<path d="M24 0H0V24" fill="none" stroke="%FG%" stroke-width="1"/>`
  },
  {
    slug: "diagonal-lines",
    title: "Diagonal lines",
    tags: ["diagonal", "lines", "stripes"],
    tile: 16,
    body: `<path d="M-4 4l8-8M0 16l16-16M12 20l8-8" stroke="%FG%" stroke-width="2"/>`
  },
  {
    slug: "checks",
    title: "Checkerboard",
    tags: ["checks", "checkerboard", "squares"],
    tile: 20,
    body: `<rect width="10" height="10" fill="%FG%"/><rect x="10" y="10" width="10" height="10" fill="%FG%"/>`
  },
  {
    slug: "waves",
    title: "Waves",
    tags: ["waves", "curves", "water"],
    tile: 40,
    body: `<path d="M0 20c10-10 30-10 40 0" fill="none" stroke="%FG%" stroke-width="2"/>`
  },
  {
    slug: "zigzag",
    title: "Zigzag",
    tags: ["zigzag", "chevron", "triangles"],
    tile: 20,
    body: `<path d="M0 10l10-10 10 10-10 10z" fill="none" stroke="%FG%" stroke-width="2"/>`
  },
  {
    slug: "cross",
    title: "Plus / cross",
    tags: ["cross", "plus", "grid"],
    tile: 24,
    body: `<path d="M12 6v12M6 12h12" stroke="%FG%" stroke-width="2"/>`
  },
  {
    slug: "circuit",
    title: "Circuit board",
    tags: ["circuit", "tech", "circuit-board"],
    tile: 40,
    body: `<path d="M0 20h12M28 20h12M20 0v12M20 28v40" stroke="%FG%" stroke-width="2" fill="none"/><circle cx="20" cy="20" r="3" fill="%FG%"/>`
  }
];
function tileToSvg(p, fg) {
  const body = p.body.replaceAll("%FG%", fg);
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${p.tile}" height="${p.tile}" viewBox="0 0 ${p.tile} ${p.tile}"><g opacity="${DEFAULT_BG_ALPHA}">${body}</g></svg>`;
}
function createPatternsProvider() {
  return {
    id: "patterns",
    kinds: [KIND4],
    license: LICENSES.bundled,
    keyless: true,
    async search(spec) {
      const q = spec.query.trim().toLowerCase();
      const fg = spec.palette?.[0] ?? DEFAULT_FG;
      const hits = q ? PATTERNS.filter((p) => p.slug.includes(q) || p.tags.some((t) => t.includes(q) || q.includes(t))) : PATTERNS;
      const pool = hits.length ? hits : PATTERNS;
      return pool.map((p, i) => {
        const raw = { slug: p.slug, fg };
        return {
          id: p.slug,
          provider: "patterns",
          kind: KIND4,
          title: p.title,
          tags: p.tags,
          width: p.tile,
          height: p.tile,
          license: LICENSES.bundled,
          score: pool.length - i,
          raw
        };
      });
    },
    async fetch(candidate) {
      const raw = candidate.raw;
      const pattern = PATTERNS.find((p) => p.slug === (raw?.slug ?? candidate.id));
      if (!pattern) throw new Error(`patterns: unknown pattern "${candidate.id}"`);
      const svg = tileToSvg(pattern, raw?.fg ?? DEFAULT_FG);
      return {
        svg,
        mime: "image/svg+xml",
        ext: "svg",
        width: pattern.tile,
        height: pattern.tile,
        kind: KIND4,
        license: candidate.license
      };
    }
  };
}

// src/assets/providers/register.ts
function toSearchContext(ctx) {
  return { env: ctx.env, timeoutMs: ctx.timeoutMs };
}
function adapt(provider) {
  return {
    id: provider.id,
    kinds: provider.kinds,
    keyless: provider.keyless,
    async search(spec, ctx) {
      let hits;
      try {
        hits = await provider.search(spec, toSearchContext(ctx));
      } catch {
        return [];
      }
      return hits.map((c) => ({
        id: c.id,
        provider: provider.id,
        kind: c.kind,
        title: c.title,
        tags: c.tags,
        w: c.width,
        h: c.height,
        url: c.sourceUrl,
        thumbUrl: c.previewUrl,
        license: c.license.name,
        attribution: c.author,
        fetch: async (fetchCtx) => {
          const fetched = await provider.fetch(c, toSearchContext(fetchCtx));
          const svgText = fetched.svg;
          const bytes = fetched.bytes ?? (svgText ? new TextEncoder().encode(svgText) : new Uint8Array());
          return { bytes, ext: "svg", mime: fetched.mime, svgText };
        }
      }));
    }
  };
}
function registerBundledAssetProviders() {
  registerAssetProvider(adapt(createIllustrationsProvider()));
  registerAssetProvider(adapt(createUndrawProvider()));
  registerAssetProvider(adapt(createMockupsProvider()));
  registerAssetProvider(adapt(createPatternsProvider()));
}
export {
  registerBundledAssetProviders
};
