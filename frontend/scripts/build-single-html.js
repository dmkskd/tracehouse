#!/usr/bin/env node
/**
 * Build Single HTML File
 *
 * Takes the Vite output from dist-single/ and inlines all JS + CSS
 * into a single self-contained HTML file.
 *
 * The JS is loaded via a blob URL so ES module syntax works from file://.
 *
 * Inspired by k8s-compass's build-single-html.js approach.
 */

import fs from 'fs'
import path from 'path'
import zlib from 'zlib'
import { fileURLToPath } from 'url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const distDir = path.resolve(__dirname, '../dist/single')
const outputFile = path.resolve(__dirname, '../dist/tracehouse.html')

// ============================================================================
// ASCII Art Helpers (adapted from k8s-compass)
// ============================================================================

const C = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  cyan: '\x1b[36m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  magenta: '\x1b[35m',
  blue: '\x1b[34m',
  red: '\x1b[31m',
  white: '\x1b[37m',
}

const BAR = { full: '█', seven: '▉', six: '▊', five: '▋', four: '▌', three: '▍', two: '▎', one: '▏', empty: '░' }

function formatBytes(bytes) {
  if (bytes >= 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(2)} MB`
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${bytes} B`
}

function createBar(percent, width = 25, color = C.cyan) {
  const filled = Math.floor(percent * width)
  const remainder = (percent * width) - filled
  let bar = color + BAR.full.repeat(filled)
  let hasPartial = false
  if (remainder >= 0.875) { bar += BAR.seven; hasPartial = true }
  else if (remainder >= 0.75) { bar += BAR.six; hasPartial = true }
  else if (remainder >= 0.625) { bar += BAR.five; hasPartial = true }
  else if (remainder >= 0.5) { bar += BAR.four; hasPartial = true }
  else if (remainder >= 0.375) { bar += BAR.three; hasPartial = true }
  else if (remainder >= 0.25) { bar += BAR.two; hasPartial = true }
  else if (remainder >= 0.125) { bar += BAR.one; hasPartial = true }
  const remaining = width - filled - (hasPartial ? 1 : 0)
  bar += C.dim + BAR.empty.repeat(Math.max(0, remaining)) + C.reset
  return bar
}

function printSizeBreakdown(items, total, title) {
  const nameW = 22, barW = 25, pctW = 7, sizeW = 10
  const contentW = nameW + barW + pctW + sizeW
  const boxW = contentW + 4

  console.log(`\n${C.bright}${C.white}+${'-'.repeat(boxW - 2)}+${C.reset}`)
  console.log(`${C.bright}${C.white}|${C.reset} ${C.bright}${title}${' '.repeat(Math.max(0, contentW - title.length))} ${C.bright}${C.white}|${C.reset}`)
  console.log(`${C.bright}${C.white}+${'-'.repeat(boxW - 2)}+${C.reset}`)

  for (const item of items) {
    const pct = item.size / total
    const pctStr = `${(pct * 100).toFixed(1)}%`.padStart(pctW)
    const sizeStr = formatBytes(item.size).padStart(sizeW)
    const bar = createBar(pct, barW, item.color || C.cyan)
    const name = item.name.length > nameW - 1
      ? item.name.slice(0, nameW - 2) + '..'
      : item.name.padEnd(nameW)
    console.log(`${C.bright}${C.white}|${C.reset} ${name}${bar}${C.dim}${pctStr}${C.reset}${sizeStr} ${C.bright}${C.white}|${C.reset}`)
  }

  console.log(`${C.bright}${C.white}+${'-'.repeat(boxW - 2)}+${C.reset}`)
  console.log(`${C.bright}${C.white}|${C.reset} ${'TOTAL'.padEnd(nameW)}${' '.repeat(barW + pctW)}${C.bright}${formatBytes(total).padStart(sizeW)}${C.reset} ${C.bright}${C.white}|${C.reset}`)
  console.log(`${C.bright}${C.white}+${'-'.repeat(boxW - 2)}+${C.reset}`)
}

function printStackedBar(items, total) {
  const barWidth = 50
  let bar = '  '
  const colors = [C.cyan, C.green, C.yellow, C.magenta, C.blue, C.red, C.white]

  items.forEach((item, i) => {
    const chars = Math.max(1, Math.round((item.size / total) * barWidth))
    bar += (item.color || colors[i % colors.length]) + BAR.full.repeat(chars) + C.reset
  })

  console.log(`\n${C.bright}  Bundle Composition${C.reset}`)
  console.log(`  ${'─'.repeat(40)}`)
  console.log(bar)
  console.log()

  items.forEach((item, i) => {
    const pct = `${((item.size / total) * 100).toFixed(1)}%`.padStart(6)
    const size = formatBytes(item.size).padStart(10)
    const color = item.color || colors[i % colors.length]
    console.log(`  ${color}█${C.reset} ${item.name.padEnd(20)} ${C.dim}${pct}${C.reset} ${size}`)
  })
}

console.log('Building single HTML file...\n')

// Read Vite output
const assetsDir = path.join(distDir, 'assets')

if (!fs.existsSync(assetsDir)) {
  console.error('Error: dist-single/assets/ not found. Run vite build first.')
  process.exit(1)
}

const jsFiles = fs.readdirSync(assetsDir).filter(f => f.endsWith('.js'))
const cssFiles = fs.readdirSync(assetsDir).filter(f => f.endsWith('.css'))

let jsContent = ''
let jsTotalSize = 0
for (const f of jsFiles) {
  const content = fs.readFileSync(path.join(assetsDir, f), 'utf-8')
  jsContent += content
  jsTotalSize += Buffer.byteLength(content, 'utf-8')
  console.log(`  ✓ JS:  ${f} (${formatBytes(Buffer.byteLength(content, 'utf-8'))})`)
}

let cssContent = ''
let cssTotalSize = 0
for (const f of cssFiles) {
  const content = fs.readFileSync(path.join(assetsDir, f), 'utf-8')
  cssContent += content
  cssTotalSize += Buffer.byteLength(content, 'utf-8')
  console.log(`  ✓ CSS: ${f} (${formatBytes(Buffer.byteLength(content, 'utf-8'))})`)
}

// Build the single HTML
const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>TraceHouse</title>
  <style>${cssContent}</style>
</head>
<body>
  <div id="root"></div>
  <script>
    // Load ES module JS via blob URL (required for file:// protocol)
    var code = ${JSON.stringify(jsContent)};
    var blob = new Blob([code], { type: 'application/javascript' });
    var url = URL.createObjectURL(blob);
    var s = document.createElement('script');
    s.type = 'module';
    s.src = url;
    document.body.appendChild(s);
  </script>
</body>
</html>`

fs.writeFileSync(outputFile, html)
const finalSize = fs.statSync(outputFile).size

// ============================================================================
// Build Compressed Versions (self-extracting HTML using DecompressionStream)
// ============================================================================

console.log('\nBuilding compressed versions...')

const htmlBuffer = Buffer.from(html, 'utf-8')
const compressed = zlib.deflateRawSync(htmlBuffer, { level: 9 })

// ---- Version 1: Web server version (raw binary, smallest) ----
const webOutputFile = path.resolve(__dirname, '../dist/tracehouse.web.html')

// Bootstrap that fetches itself and decompresses - only works over HTTP
const webBootstrapTemplate = `<svg onload="fetch(location.href).then(r=>r.blob()).then(b=>new Response(b.slice(BOOTSTRAP_SIZE).stream().pipeThrough(new DecompressionStream('deflate-raw'))).text()).then(h=>{document.open();document.write(h);document.close()})"><!--`

// Calculate actual bootstrap size and rebuild with correct offset
const placeholderSize = Buffer.byteLength(webBootstrapTemplate.replace('BOOTSTRAP_SIZE', '000'), 'utf-8')
const finalWebBootstrap = webBootstrapTemplate.replace('BOOTSTRAP_SIZE', String(placeholderSize).padStart(3, '0'))

const webCompressedHtml = Buffer.concat([
  Buffer.from(finalWebBootstrap, 'utf-8'),
  compressed
])

fs.writeFileSync(webOutputFile, webCompressedHtml)
const webCompressedSize = webCompressedHtml.length
const webCompressionRatio = ((1 - webCompressedSize / finalSize) * 100).toFixed(1)

console.log(`  ✓ Web version: ${formatBytes(finalSize)} → ${formatBytes(webCompressedSize)} (${webCompressionRatio}% smaller)`)
console.log(`    Bootstrap: ${placeholderSize} bytes, requires HTTP server`)

// ---- Version 2: File:// version (base64 encoded, works offline) ----
const compressedOutputFile = path.resolve(__dirname, '../dist/tracehouse.compressed.html')

const compressedBase64 = compressed.toString('base64')

const compressedHtmlContent = `<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body>
<script>
(async()=>{
  const b64="${compressedBase64}";
  const bin=Uint8Array.from(atob(b64),c=>c.charCodeAt(0));
  const ds=new DecompressionStream('deflate-raw');
  const writer=ds.writable.getWriter();
  writer.write(bin);
  writer.close();
  const chunks=[];
  const reader=ds.readable.getReader();
  while(true){
    const{done,value}=await reader.read();
    if(done)break;
    chunks.push(value);
  }
  const html=new TextDecoder().decode(await new Blob(chunks).arrayBuffer());
  document.open();
  document.write(html);
  document.close();
})();
</script>
</body>
</html>`

fs.writeFileSync(compressedOutputFile, compressedHtmlContent)
const compressedSize = Buffer.byteLength(compressedHtmlContent, 'utf-8')
const compressionRatio = ((1 - compressedSize / finalSize) * 100).toFixed(1)

console.log(`  ✓ File version: ${formatBytes(finalSize)} → ${formatBytes(compressedSize)} (${compressionRatio}% smaller)`)

// ============================================================================
// Library Size Breakdown (from Vite module-sizes.json)
// ============================================================================

const moduleSizesPath = path.join(distDir, 'module-sizes.json')
if (fs.existsSync(moduleSizesPath)) {
  const modules = JSON.parse(fs.readFileSync(moduleSizesPath, 'utf-8'))

  // Aggregate by top-level package
  const pkgSizes = {}
  let appCodeSize = 0
  for (const [id, size] of Object.entries(modules)) {
    const m = id.match(/node_modules\/(@[^/]+\/[^/]+|[^/]+)/)
    if (m) {
      pkgSizes[m[1]] = (pkgSizes[m[1]] || 0) + size
    } else {
      appCodeSize += size
    }
  }

  const sorted = Object.entries(pkgSizes).sort((a, b) => b[1] - a[1])
  const top = sorted.slice(0, 8)

  const topItems = top.map(([name, size]) => ({ name, size, color: C.yellow }))
  topItems.push({ name: 'Other packages', size: sorted.slice(8).reduce((s, [, v]) => s + v, 0), color: C.dim })
  topItems.push({ name: 'App code', size: appCodeSize, color: C.green })

  printSizeBreakdown(topItems, jsTotalSize, 'JS BUNDLE BREAKDOWN (top libraries)')

  // Clean up
  fs.unlinkSync(moduleSizesPath)
}

// ============================================================================
// Self-Containment Check (no external dependencies)
// ============================================================================

const externalResourcePattern = /<(?:script|link|img)\s[^>]*(?:src|href)\s*=\s*["']https?:\/\/[^"']+["']/gi
const externalMatches = html.match(externalResourcePattern) || []

// Also check for dynamic imports from external URLs
const dynamicImportPattern = /import\s*\([^)]*["']https?:\/\/[^"']+["']\s*\)/g
const dynamicMatches = html.match(dynamicImportPattern) || []

const allExternal = [...externalMatches, ...dynamicMatches]

if (allExternal.length === 0) {
  console.log(`\n${C.green}  SELF-CONTAINED: No external resource loading detected${C.reset}`)
  console.log(`${C.dim}     Bundle loads zero external scripts, stylesheets, or images${C.reset}`)
} else {
  console.log(`\n${C.red}  WARNING: EXTERNAL DEPENDENCIES DETECTED:${C.reset}`)
  for (const match of allExternal) {
    console.log(`${C.red}     ${match.slice(0, 100)}${C.reset}`)
  }
  process.exitCode = 1
}

// ============================================================================
// Bundle Size Report
// ============================================================================

const htmlBoilerplate = finalSize - jsTotalSize - cssTotalSize

console.log(`\n${C.bright}${C.cyan}`)
console.log(`  ╭─────────────────────────────────────────────────────────────╮`)
console.log(`  │                    BUNDLE SIZE REPORT                       │`)
console.log(`  ╰─────────────────────────────────────────────────────────────╯${C.reset}`)

// File composition breakdown
const mainComponents = [
  { name: 'JavaScript Bundle', size: jsTotalSize, color: C.yellow },
  { name: 'CSS Styles', size: cssTotalSize, color: C.green },
  { name: 'HTML Boilerplate', size: htmlBoilerplate, color: C.blue },
]

printSizeBreakdown(mainComponents, finalSize, 'FILE COMPOSITION (standard)')

// Stacked bar
printStackedBar(mainComponents, finalSize)

// Compression comparison
const compressedDeflateSize = compressed.length
const compressedItems = [
  { name: 'Standard', size: finalSize, color: C.yellow },
  { name: 'Compressed (file://)', size: compressedSize, color: C.green },
  { name: 'Web (HTTP only)', size: webCompressedSize, color: C.cyan },
]

printSizeBreakdown(compressedItems, finalSize, 'OUTPUT VARIANTS')

console.log(`\n${C.dim}  Raw deflate: ${formatBytes(compressedDeflateSize)}  (before base64/bootstrap overhead)${C.reset}`)

// Final summary
console.log(`\n${C.bright}${C.green}  BUILD COMPLETE${C.reset}`)
console.log(`  ${'─'.repeat(55)}`)
console.log(`  ${C.dim}Standard:${C.reset}     ${path.basename(outputFile)}`)
console.log(`                ${C.bright}${formatBytes(finalSize)}${C.reset} ${C.dim}(works everywhere)${C.reset}`)
console.log(`  ${C.dim}Compressed:${C.reset}   ${path.basename(compressedOutputFile)}`)
console.log(`                ${C.bright}${formatBytes(compressedSize)}${C.reset} ${C.green}(${compressionRatio}% smaller)${C.reset} ${C.dim}file:// OK${C.reset}`)
console.log(`  ${C.dim}Web:${C.reset}          ${path.basename(webOutputFile)}`)
console.log(`                ${C.bright}${formatBytes(webCompressedSize)}${C.reset} ${C.green}(${webCompressionRatio}% smaller)${C.reset} ${C.dim}HTTP only${C.reset}`)

console.log(`
${C.dim}  ┌────────────────────────────────────────────┐
  │  ${C.cyan}TraceHouse${C.dim} — Single-file build    │
  │  ${C.reset}${C.dim}Self-contained, offline-capable, fast     │
  └────────────────────────────────────────────┘${C.reset}
`)
