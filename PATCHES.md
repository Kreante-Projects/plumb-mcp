# Kreante security patch for the Figma plugin

This branch changes only `figma-plugin/ui.html` and `figma-plugin/code.js`, on top of the
unmodified 0.13.2 backup. The MCP server in `dist/` and `archive/` are unchanged.

| Issue | Change |
|---|---|
| Unpaired servers could send requests to the plugin worker | Requests and uploads now require a paired session |
| An upload without a live session could wait forever | The UI always acknowledges it, and the worker times out after 60 seconds |
| Prototype links accepted any URL scheme | Only well-formed `http` and `https` URLs are written |

## Known limitation

Pairing still has no shared secret. After the first pairing the plugin pairs automatically
with any local server that answers on ports 31337-31346. Fixing that requires a server
change, and the server source is not available.

## Test

```bash
node --test tests/
```

## Install

In the Figma desktop app, use Plugins > Development > Import plugin from manifest and select
`figma-plugin/manifest.json` from this branch.
