# Building

## Single-File Build

Build the entire app as one self-contained HTML file - no server needed, works from `file://`:

```bash
just build-single
```

This runs `npm run build:single` in the frontend workspace and produces three variants in `frontend/dist/`:

| File | Size | Use case |
|------|------|----------|
| `tracehouse.html` | ~3.6 MB | Standard - works everywhere including `file://` |
| `tracehouse.compressed.html` | ~1.3 MB | Compressed with self-extracting bootstrap - works from `file://` too |
| `tracehouse.web.html` | ~1 MB | Smallest - HTTP servers only |

### Build Output

```
Building single HTML file...

  ✓ JS:  index-CHjc9aCj.js (3.41 MB)
  ✓ CSS: style-BAJtgBMR.css (76.8 KB)

Building compressed versions...
  ✓ Web version: 3.59 MB → 998.0 KB (72.9% smaller)
    Bootstrap: 223 bytes, requires HTTP server
  ✓ File version: 3.59 MB → 1.30 MB (63.8% smaller)

+------------------------------------------------------------------+
| JS BUNDLE BREAKDOWN (top libraries)                              |
+------------------------------------------------------------------+
| three                 ██████████████▏░░░░░░░░░░  56.5%   1.93 MB |
| highlight.js          ██████████▏░░░░░░░░░░░░░░  40.6%   1.38 MB |
| recharts              ████▊░░░░░░░░░░░░░░░░░░░░  19.1%  665.6 KB |
| d3-flame-graph        █▌░░░░░░░░░░░░░░░░░░░░░░░   6.5%  226.8 KB |
| react-dom             ▉░░░░░░░░░░░░░░░░░░░░░░░░   3.9%  136.5 KB |
| react-reconciler      ▋░░░░░░░░░░░░░░░░░░░░░░░░   2.7%   93.6 KB |
| @clickhouse/client-c..▌░░░░░░░░░░░░░░░░░░░░░░░░   2.3%   78.8 KB |
| @react-three/fiber    ▍░░░░░░░░░░░░░░░░░░░░░░░░   1.9%   66.6 KB |
| Other packages        ████▊░░░░░░░░░░░░░░░░░░░░  19.4%  678.2 KB |
| App code              █████████████▋░░░░░░░░░░░  54.9%   1.87 MB |
+------------------------------------------------------------------+
| TOTAL                                                    3.41 MB |
+------------------------------------------------------------------+

  SELF-CONTAINED: No external resource loading detected
     Bundle loads zero external scripts, stylesheets, or images


  ╭─────────────────────────────────────────────────────────────╮
  │                    BUNDLE SIZE REPORT                       │
  ╰─────────────────────────────────────────────────────────────╯

+------------------------------------------------------------------+
| FILE COMPOSITION (standard)                                      |
+------------------------------------------------------------------+
| JavaScript Bundle     ███████████████████████▋░  94.9%   3.41 MB |
| CSS Styles            ▌░░░░░░░░░░░░░░░░░░░░░░░░   2.1%   76.8 KB |
| HTML Boilerplate      ▊░░░░░░░░░░░░░░░░░░░░░░░░   3.0%  110.6 KB |
+------------------------------------------------------------------+
| TOTAL                                                    3.59 MB |
+------------------------------------------------------------------+

  Bundle Composition
  ────────────────────────────────────────
  ██████████████████████████████████████████████████

  █ JavaScript Bundle     94.9%    3.41 MB
  █ CSS Styles             2.1%    76.8 KB
  █ HTML Boilerplate       3.0%   110.6 KB

+------------------------------------------------------------------+
| OUTPUT VARIANTS                                                  |
+------------------------------------------------------------------+
| Standard              █████████████████████████ 100.0%   3.59 MB |
| Compressed (file://)  █████████░░░░░░░░░░░░░░░░  36.2%   1.30 MB |
| Web (HTTP only)       ██████▊░░░░░░░░░░░░░░░░░░  27.1%  998.0 KB |
+------------------------------------------------------------------+
| TOTAL                                                    3.59 MB |
+------------------------------------------------------------------+

  Raw deflate: 997.8 KB  (before base64/bootstrap overhead)

  BUILD COMPLETE
  ───────────────────────────────────────────────────────
  Standard:     tracehouse.html
                3.59 MB (works everywhere)
  Compressed:   tracehouse.compressed.html
                1.30 MB (63.8% smaller) file:// OK
  Web:          tracehouse.web.html
                998.0 KB (72.9% smaller) HTTP only

  ┌────────────────────────────────────────────┐
  │  TraceHouse - Single-file build    │
  │  Self-contained, offline-capable, fast     │
  └────────────────────────────────────────────┘
```
