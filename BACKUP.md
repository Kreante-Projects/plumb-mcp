# Backup of plumb-mcp 0.13.2

This repository is an unmodified backup of the `plumb-mcp` npm package, version 0.13.2.
It is not a fork under active development. The original author's work is distributed
under the MIT License; see `LICENSE`.

## Why it exists

The upstream repository (`github.com/tathagat22/plumb-mcp`) and its documentation site
returned 404 on 2026-11-18. The package was still available on npm that day.

## Provenance

| Field | Value |
|---|---|
| Source | https://registry.npmjs.org/plumb-mcp/-/plumb-mcp-0.13.2.tgz |
| Retrieved | 2026-11-18 |
| npm integrity | `sha512-OU9YLqknbUSYP5HjWa5DZLaHf3TAzvNZaNKd0biR1b5t3POjMaLnJSYUB0rnWrwS1PxFStEdT204/Dbt8uGiag==` |
| Tarball SHA-256 | `877c41ac27f3565cd8a532811c4cc18bcd9860ba14feff2b3533cf300891e67c` |

## Contents

- The repository root holds the extracted package, byte for byte. Only `BACKUP.md` and `archive/` were added.
- `archive/plumb-mcp-0.13.2.tgz` is the original npm tarball.

This is the published build output. The TypeScript source was not part of the npm package.

## Restore

```bash
shasum -a 256 archive/plumb-mcp-0.13.2.tgz
npm install -g ./archive/plumb-mcp-0.13.2.tgz
```

Runtime dependencies (`@modelcontextprotocol/sdk`, `openfig-core`, `ws`, `zod`) are not
included and are still resolved from npm.
