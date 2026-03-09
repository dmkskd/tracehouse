import { useState, useEffect, useMemo } from "react";

// ============================================================
// COLOR SYSTEM — same as Live View for consistency
// ============================================================
const C = {
  bg: "#0f172a", bgCard: "#1e293b", bgDeep: "#0b1120",
  border: "#334155", borderLight: "#475569",
  text: "#e2e8f0", textMuted: "#94a3b8", textDim: "#64748b",
  // Semantic
  queries: "#3b82f6", merges: "#f59e0b", mutations: "#ef4444",
  replication: "#8b5cf6", mvs: "#06b6d4",
  // Memory subsystems
  markCache: "#22d3ee",    // cyan
  uncompCache: "#14b8a6",  // teal
  primaryKey: "#a78bfa",   // violet
  dictionaries: "#fb923c", // orange
  hashTables: "#f472b6",   // pink
  mergeBuffers: "#fbbf24",  // amber
  queryMem: "#60a5fa",     // light blue
  jemalloc: "#818cf8",     // indigo
  osPageCache: "#475569",  // slate
  free: "#1e293b",
  // CPU states
  cpuUser: "#3b82f6", cpuSystem: "#ef4444", cpuIOWait: "#f59e0b",
  cpuIdle: "#1e293b", cpuQuery: "#3b82f6", cpuMerge: "#f59e0b",
  cpuMutation: "#ef4444", cpuRepl: "#8b5cf6",
  // Health
  ok: "#22c55e", warn: "#f59e0b", crit: "#ef4444",
};

// ============================================================
// MOCK DATA — Deep internals
// ============================================================

const TOTAL_RAM_GB = 64;

const MEMORY_MAP = {
  totalRSS: 48.2,
  jemalloc: { allocated: 42.1, resident: 48.2, mapped: 52.4, retained: 4.3, metadata: 0.8 },
  subsystems: [
    { id: "query_mem",   label: "Query Working Memory",    gb: 22.1, color: C.queryMem,    detail: "Hash tables, sort buffers, JOIN build sides, intermediate blocks", icon: "⚡" },
    { id: "mark_cache",  label: "Mark Cache",              gb: 6.2,  color: C.markCache,   detail: "Granule offset index — maps granule_id → (block_offset, row_offset) per column file", icon: "📍",
      sub: { files: 14820, hitRate: 98.7, misses_sec: 12, configured: 8.0 } },
    { id: "uncomp_cache",label: "Uncompressed Block Cache", gb: 2.1, color: C.uncompCache, detail: "Recently decompressed column blocks (8192-row granules, post-LZ4/ZSTD)", icon: "📦",
      sub: { cells: 8430, hitRate: 34.2, misses_sec: 890, configured: 4.0 } },
    { id: "pk_index",    label: "Primary Key Index",        gb: 3.4, color: C.primaryKey,  detail: "Sparse index: first-row PK values per granule, loaded for all active parts", icon: "🔑",
      sub: { tables: 47, allocated: 4.1 } },
    { id: "dictionaries",label: "Dictionaries",             gb: 1.1, color: C.dictionaries, detail: "In-memory key-value lookup tables (hashed, flat, complex_key)", icon: "📖",
      sub: { count: 3, names: ["geo_regions", "device_types", "currency_rates"] } },
    { id: "merge_buf",   label: "Merge Buffers",            gb: 2.8, color: C.mergeBuffers, detail: "Read + write buffers for active background merges (k-way merge sort)", icon: "🔀" },
    { id: "jemalloc_overhead", label: "jemalloc Overhead",  gb: 1.8, color: C.jemalloc,    detail: "Fragmentation, thread caches, retained arenas, metadata", icon: "🧱" },
    { id: "other",       label: "Other / Untracked",        gb: 1.5, color: C.textDim,     detail: "Connections, parsed ASTs, compiled expressions, filesystem cache mapping", icon: "…" },
  ],
  osPageCache: 12.4,
  free: 3.4,
};

// Per-query internal breakdown
const QUERY_INTERNALS = [
  {
    id: "3f60b9d4", kind: "SELECT", user: "analytics", elapsed: 441.5,
    query: "SELECT a.country_code, b.device_type, count() ... GROUP BY 1, 2",
    totalMem: 12.4,
    breakdown: [
      { component: "Hash Table (GROUP BY)", gb: 6.8, detail: "Two-level hash table, 256 sub-tables, ~45M unique groups", color: C.hashTables },
      { component: "JOIN Build Side", gb: 3.2, detail: "Hash join build from devices table — 2.1M rows, parallel 8 lanes", color: "#e879f9" },
      { component: "Read Buffers", gb: 1.6, detail: "Decompression buffers for 4 parallel scan threads × 2 column files", color: C.uncompCache },
      { component: "Sort Buffer", gb: 0.5, detail: "Partial sort for ORDER BY 3 DESC, spilling to disk at 8GB limit", color: "#fcd34d" },
      { component: "Network Send", gb: 0.3, detail: "Output serialization buffer", color: C.textDim },
    ],
    pipeline: [
      { stage: "Scan", threads: 4, status: "active", detail: "ReadFromMergeTree: 2.1B rows, 1847 parts, SelectedMarks: 42K of 890K (95% pruned)" },
      { stage: "Filter", threads: 4, status: "active", detail: "WHERE date >= '2026-01-01' — selectivity 34%, column-by-column eval" },
      { stage: "Hash Join", threads: 4, status: "active", detail: "INNER JOIN devices — probe phase, 8 hash table partitions" },
      { stage: "Aggregate", threads: 4, status: "active", detail: "GROUP BY country_code, device_type — two-level hash table, 45M groups" },
      { stage: "MergeAgg", threads: 1, status: "waiting", detail: "GroupStateMerge — pipeline breaker, waiting for all Aggregate lanes" },
      { stage: "Sort", threads: 0, status: "pending", detail: "ORDER BY count() DESC — not yet started" },
    ],
    profileEvents: {
      UserTimeMicroseconds: 1_847_000_000, // 1847s across all threads
      SystemTimeMicroseconds: 234_000_000,
      RealTimeMicroseconds: 441_500_000,
      OSIOWaitMicroseconds: 89_000_000,
      ReadCompressedBytes: 148_000_000_000,
      SelectedParts: 1847, SelectedMarks: 42000, TotalMarks: 890000,
      MarkCacheHits: 41200, MarkCacheMisses: 800,
    },
    threads: 4, maxThreads: 16,
  },
  {
    id: "83e4b0dc", kind: "SELECT", user: "default", elapsed: 87.2,
    query: "SELECT toStartOfHour(timestamp) AS hour, uniqExact(user_id) ...",
    totalMem: 4.2,
    breakdown: [
      { component: "HyperLogLog States", gb: 2.8, detail: "uniqExact — maintaining exact set of 12M+ unique user_ids in hash set", color: C.hashTables },
      { component: "Read Buffers", gb: 0.9, detail: "2 threads × pageviews column files (timestamp, user_id)", color: C.uncompCache },
      { component: "Output Block", gb: 0.5, detail: "168 hour-buckets × aggregation state", color: C.textDim },
    ],
    pipeline: [
      { stage: "Scan", threads: 2, status: "active", detail: "ReadFromMergeTree: 890M rows, SelectedMarks: 12K of 340K (96% pruned by PK on date)" },
      { stage: "Aggregate", threads: 2, status: "active", detail: "GROUP BY toStartOfHour — sort aggregation (PK prefix match!), streaming" },
      { stage: "Merge", threads: 1, status: "waiting", detail: "Merging partial states from 2 lanes" },
    ],
    profileEvents: {
      UserTimeMicroseconds: 312_000_000,
      SystemTimeMicroseconds: 45_000_000,
      RealTimeMicroseconds: 87_200_000,
      OSIOWaitMicroseconds: 22_000_000,
      ReadCompressedBytes: 34_000_000_000,
      SelectedParts: 234, SelectedMarks: 12000, TotalMarks: 340000,
      MarkCacheHits: 11800, MarkCacheMisses: 200,
    },
    threads: 2, maxThreads: 16,
  },
];

// Per-core CPU state (16 cores)
const CPU_CORES = Array.from({ length: 16 }, (_, i) => {
  const states = [
    { pct: 78, state: "user", owner: "query:3f60b9d4" },
    { pct: 45, state: "user", owner: "query:3f60b9d4" },
    { pct: 92, state: "user", owner: "query:83e4b0dc" },
    { pct: 12, state: "idle", owner: null },
    { pct: 67, state: "user", owner: "query:3f60b9d4" },
    { pct: 88, state: "user", owner: "merge:events" },
    { pct: 34, state: "iowait", owner: "merge:pageviews" },
    { pct: 95, state: "user", owner: "query:83e4b0dc" },
    { pct: 56, state: "system", owner: "query:bfc0a86c" },
    { pct: 71, state: "user", owner: "merge:events" },
    { pct: 23, state: "user", owner: "mutation:events_old" },
    { pct: 8, state: "idle", owner: null },
    { pct: 82, state: "user", owner: "query:3f60b9d4" },
    { pct: 43, state: "user", owner: "replication" },
    { pct: 61, state: "user", owner: "query:bfc0a86c" },
    { pct: 5, state: "idle", owner: null },
  ];
  return { core: i, ...states[i] };
});

// Thread pools
const THREAD_POOLS = [
  { name: "Query Execution", active: 38, max: 64, color: C.queries, metric: "QueryThread" },
  { name: "Merges & Mutations", active: 6, max: 16, color: C.merges, metric: "BackgroundMergesAndMutationsPoolTask" },
  { name: "Replication Fetches", active: 2, max: 8, color: C.replication, metric: "BackgroundFetchesPoolTask" },
  { name: "Schedule Pool", active: 3, max: 16, color: C.mvs, metric: "BackgroundSchedulePoolTask" },
  { name: "IO Thread Pool", active: 8, max: 32, color: C.warn, metric: "IOThreads" },
];

// Top tables by PK memory
const PK_BY_TABLE = [
  { table: "events", pkMem: 1.8e9, parts: 1847, rows: "12.4B", granules: 890000 },
  { table: "pageviews", pkMem: 0.9e9, parts: 456, rows: "3.2B", granules: 340000 },
  { table: "user_profiles", pkMem: 0.3e9, parts: 23, rows: "180M", granules: 21000 },
  { table: "events_daily", pkMem: 0.12e9, parts: 89, rows: "45M", granules: 5400 },
  { table: "system.metric_log", pkMem: 0.08e9, parts: 312, rows: "28M", granules: 3400 },
];

// ============================================================
// UTILITY
// ============================================================
function fmt(gb) {
  if (gb >= 1) return gb.toFixed(1) + " GB";
  if (gb >= 0.001) return (gb * 1024).toFixed(0) + " MB";
  return (gb * 1024 * 1024).toFixed(0) + " KB";
}
function fmtBytes(b) {
  if (b >= 1e12) return (b/1e12).toFixed(1) + " TB";
  if (b >= 1e9) return (b/1e9).toFixed(1) + " GB";
  if (b >= 1e6) return (b/1e6).toFixed(0) + " MB";
  return (b/1e3).toFixed(0) + " KB";
}
function fmtDur(s) {
  if (s >= 3600) return Math.floor(s/3600)+"h"+Math.floor((s%3600)/60)+"m";
  if (s >= 60) return Math.floor(s/60)+"m"+Math.floor(s%60)+"s";
  return s.toFixed(1)+"s";
}
function fmtRate(n) { return n >= 1000 ? (n/1000).toFixed(1)+"K" : n.toString(); }
function pct(v, total) { return ((v / total) * 100).toFixed(1); }

const MONO = "'JetBrains Mono','SF Mono','Fira Code','Cascadia Code',monospace";

// ============================================================
// COMPONENTS
// ============================================================

function Card({ children, style }) {
  return <div style={{ background: C.bgCard, borderRadius: 10, border: `1px solid ${C.border}`, padding: "14px 16px", ...style }}>{children}</div>;
}

function Label({ children, sub }) {
  return (
    <div style={{ marginBottom: 10 }}>
      <span style={{ fontSize: 12, fontWeight: 700, color: C.text, textTransform: "uppercase", letterSpacing: "0.04em" }}>{children}</span>
      {sub && <span style={{ fontSize: 10, color: C.textDim, marginLeft: 8 }}>{sub}</span>}
    </div>
  );
}

// --- Memory waterfall ---
function MemoryWaterfall({ subsystems, totalRAM }) {
  return (
    <div>
      {/* The main bar */}
      <div style={{ display: "flex", height: 28, borderRadius: 5, overflow: "hidden", border: `1px solid ${C.border}`, marginBottom: 8 }}>
        {subsystems.map((s, i) => {
          const w = (s.gb / totalRAM) * 100;
          if (w < 0.3) return null;
          return (
            <div key={i} title={`${s.label}: ${fmt(s.gb)} (${pct(s.gb, totalRAM)}%)`} style={{
              width: w + "%", background: s.color, opacity: 0.8,
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 8, fontWeight: 700, color: "#fff", whiteSpace: "nowrap", overflow: "hidden",
              borderRight: "1px solid rgba(0,0,0,0.3)",
              transition: "width 0.5s ease",
            }}>
              {w > 5 ? `${s.label.split(" ")[0]} ${s.gb.toFixed(1)}G` : w > 2.5 ? `${s.gb.toFixed(1)}G` : ""}
            </div>
          );
        })}
        {/* OS page cache */}
        <div title={`OS Page Cache: ${fmt(MEMORY_MAP.osPageCache)}`} style={{
          width: (MEMORY_MAP.osPageCache / totalRAM) * 100 + "%", background: C.osPageCache, opacity: 0.5,
          display: "flex", alignItems: "center", justifyContent: "center",
          fontSize: 8, color: "#94a3b8", borderRight: "1px solid rgba(0,0,0,0.3)",
        }}>
          PageCache {MEMORY_MAP.osPageCache.toFixed(0)}G
        </div>
        <div style={{ flex: 1, background: C.free }} />
      </div>

      {/* Detailed breakdown table */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 3 }}>
        {subsystems.map((s, i) => (
          <div key={i} style={{
            display: "grid", gridTemplateColumns: "20px 200px 70px 48px 1fr",
            alignItems: "center", gap: 8, padding: "5px 6px", borderRadius: 4,
            background: "rgba(255,255,255,0.02)", fontSize: 11,
            borderLeft: `3px solid ${s.color}`,
            cursor: "pointer",
          }}
          >
            <span style={{ fontSize: 12 }}>{s.icon}</span>
            <span style={{ color: C.text, fontWeight: 600 }}>{s.label}</span>
            <span style={{ fontFamily: MONO, color: s.color, fontWeight: 700, textAlign: "right" }}>{fmt(s.gb)}</span>
            <span style={{ fontFamily: MONO, color: C.textDim, fontSize: 10, textAlign: "right" }}>{pct(s.gb, totalRAM)}%</span>
            <span style={{ fontSize: 9, color: C.textDim, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{s.detail}</span>
          </div>
        ))}
      </div>

      {/* Cache hit rates inline */}
      <div style={{ display: "flex", gap: 16, marginTop: 10, fontSize: 10, color: C.textMuted, flexWrap: "wrap" }}>
        <span>Mark Cache hit rate: <span style={{ fontFamily: MONO, color: MEMORY_MAP.subsystems[1].sub.hitRate > 95 ? C.ok : C.warn, fontWeight: 700 }}>{MEMORY_MAP.subsystems[1].sub.hitRate}%</span>
          <span style={{ color: C.textDim }}> ({fmtRate(MEMORY_MAP.subsystems[1].sub.misses_sec)} misses/s, {MEMORY_MAP.subsystems[1].sub.files.toLocaleString()} files, {fmt(MEMORY_MAP.subsystems[1].sub.configured)} configured)</span>
        </span>
        <span>Uncomp Cache hit rate: <span style={{ fontFamily: MONO, color: MEMORY_MAP.subsystems[2].sub.hitRate > 50 ? C.ok : C.warn, fontWeight: 700 }}>{MEMORY_MAP.subsystems[2].sub.hitRate}%</span>
          <span style={{ color: C.textDim }}> ({fmtRate(MEMORY_MAP.subsystems[2].sub.misses_sec)} misses/s, {MEMORY_MAP.subsystems[2].sub.cells.toLocaleString()} cells, {fmt(MEMORY_MAP.subsystems[2].sub.configured)} configured)</span>
        </span>
      </div>
    </div>
  );
}

// --- CPU Core Heatmap ---
function CoreHeatmap({ cores }) {
  function coreColor(core) {
    if (core.state === "idle") return C.cpuIdle;
    if (core.state === "iowait") return C.cpuIOWait;
    if (core.state === "system") return C.cpuSystem;
    // User — color by owner type
    if (core.owner?.startsWith("query")) return C.cpuQuery;
    if (core.owner?.startsWith("merge")) return C.cpuMerge;
    if (core.owner?.startsWith("mutation")) return C.cpuMutation;
    if (core.owner === "replication") return C.cpuRepl;
    return C.cpuUser;
  }
  function ownerLabel(core) {
    if (!core.owner) return "idle";
    if (core.owner.startsWith("query:")) return core.owner.split(":")[1].substring(0,8);
    return core.owner;
  }

  return (
    <div>
      <div style={{ display: "grid", gridTemplateColumns: `repeat(${Math.min(cores.length, 16)}, 1fr)`, gap: 4 }}>
        {cores.map((core, i) => {
          const color = coreColor(core);
          const intensity = core.pct / 100;
          return (
            <div key={i} title={`Core ${i}: ${core.pct}% ${core.state} — ${core.owner || "idle"}`} style={{
              borderRadius: 5,
              background: `linear-gradient(180deg, ${color}${Math.round(intensity * 200 + 55).toString(16).padStart(2,"0")} 0%, ${C.bgDeep} 100%)`,
              border: `1px solid ${color}${Math.round(intensity * 150 + 30).toString(16).padStart(2,"0")}`,
              padding: "6px 2px 4px",
              textAlign: "center",
              transition: "all 0.3s ease",
            }}>
              <div style={{ fontSize: 7, color: C.textDim, fontFamily: MONO }}>C{i}</div>
              <div style={{ fontSize: 14, fontWeight: 800, fontFamily: MONO, color: core.pct > 80 ? "#fff" : core.pct > 40 ? C.text : C.textDim }}>
                {core.pct}
              </div>
              <div style={{ fontSize: 7, fontWeight: 600, color: color, fontFamily: MONO, marginTop: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                {ownerLabel(core)}
              </div>
            </div>
          );
        })}
      </div>
      {/* Legend */}
      <div style={{ display: "flex", gap: 12, marginTop: 8, flexWrap: "wrap" }}>
        {[
          { color: C.cpuQuery, label: "Query" }, { color: C.cpuMerge, label: "Merge" },
          { color: C.cpuMutation, label: "Mutation" }, { color: C.cpuRepl, label: "Replication" },
          { color: C.cpuSystem, label: "Kernel/Sys" }, { color: C.cpuIOWait, label: "IO Wait" },
          { color: C.cpuIdle, label: "Idle" },
        ].map((l, i) => (
          <div key={i} style={{ display: "flex", alignItems: "center", gap: 3, fontSize: 9, color: C.textMuted }}>
            <div style={{ width: 8, height: 8, borderRadius: 2, background: l.color, opacity: l.label === "Idle" ? 0.3 : 0.8 }} />
            {l.label}
          </div>
        ))}
      </div>
    </div>
  );
}

// --- Thread Pool Bars ---
function ThreadPoolViz({ pools }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 5 }}>
      {pools.map((p, i) => (
        <div key={i} style={{ display: "grid", gridTemplateColumns: "160px 1fr 50px", gap: 8, alignItems: "center" }}>
          <span style={{ fontSize: 10, color: C.textMuted, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{p.name}</span>
          <div style={{ height: 14, background: C.bgDeep, borderRadius: 3, overflow: "hidden", position: "relative" }}>
            <div style={{
              width: (p.active / p.max) * 100 + "%", height: "100%", background: p.color,
              opacity: 0.7, borderRadius: 3, transition: "width 0.4s ease",
            }} />
            {p.active / p.max > 0.8 && (
              <div style={{ position: "absolute", right: 4, top: 0, bottom: 0, display: "flex", alignItems: "center", fontSize: 8, color: C.crit, fontWeight: 700 }}>SATURATED</div>
            )}
          </div>
          <span style={{ fontFamily: MONO, fontSize: 10, color: C.text, textAlign: "right" }}>
            <span style={{ color: p.active / p.max > 0.8 ? C.warn : C.text, fontWeight: 700 }}>{p.active}</span>
            <span style={{ color: C.textDim }}>/{p.max}</span>
          </span>
        </div>
      ))}
    </div>
  );
}

// --- Query Internal Breakdown ---
function QueryInternalsCard({ q }) {
  const totalCpuSec = (q.profileEvents.UserTimeMicroseconds + q.profileEvents.SystemTimeMicroseconds) / 1e6;
  const wallSec = q.profileEvents.RealTimeMicroseconds / 1e6;
  const parallelism = totalCpuSec / wallSec;
  const ioWaitPct = q.profileEvents.OSIOWaitMicroseconds / (q.profileEvents.UserTimeMicroseconds + q.profileEvents.SystemTimeMicroseconds + q.profileEvents.OSIOWaitMicroseconds) * 100;
  const prunedPct = ((q.profileEvents.TotalMarks - q.profileEvents.SelectedMarks) / q.profileEvents.TotalMarks * 100);
  const markHitRate = q.profileEvents.MarkCacheHits / (q.profileEvents.MarkCacheHits + q.profileEvents.MarkCacheMisses) * 100;

  return (
    <Card style={{ borderLeft: `3px solid ${C.queries}` }}>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 10 }}>
        <div>
          <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 3 }}>
            <span style={{ fontFamily: MONO, fontSize: 12, color: C.queries, fontWeight: 700 }}>{q.id}</span>
            <span style={{ fontSize: 8, fontWeight: 700, padding: "1px 5px", borderRadius: 3, background: C.queries+"20", color: C.queries }}>{q.kind}</span>
            <span style={{ fontSize: 10, color: C.textDim }}>{q.user}</span>
            <span style={{ fontFamily: MONO, fontSize: 10, color: C.textMuted }}>{fmtDur(q.elapsed)}</span>
          </div>
          <div style={{ fontFamily: MONO, fontSize: 9, color: C.textDim, maxWidth: 500, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{q.query}</div>
        </div>
        <div style={{ textAlign: "right" }}>
          <div style={{ fontSize: 18, fontWeight: 800, fontFamily: MONO, color: C.text }}>{fmt(q.totalMem)}</div>
          <div style={{ fontSize: 9, color: C.textDim }}>total memory</div>
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        {/* Left: Memory breakdown */}
        <div>
          <div style={{ fontSize: 9, fontWeight: 700, color: C.textDim, textTransform: "uppercase", marginBottom: 6, letterSpacing: "0.06em" }}>Memory Anatomy</div>
          {/* Stacked bar */}
          <div style={{ display: "flex", height: 18, borderRadius: 3, overflow: "hidden", marginBottom: 6, border: `1px solid ${C.border}` }}>
            {q.breakdown.map((b, i) => {
              const w = (b.gb / q.totalMem) * 100;
              return (
                <div key={i} title={`${b.component}: ${fmt(b.gb)}`} style={{
                  width: w + "%", background: b.color, opacity: 0.8,
                  borderRight: i < q.breakdown.length - 1 ? "1px solid rgba(0,0,0,0.3)" : "none",
                }} />
              );
            })}
          </div>
          {q.breakdown.map((b, i) => (
            <div key={i} style={{ display: "grid", gridTemplateColumns: "12px 1fr 55px", gap: 4, alignItems: "start", marginBottom: 3 }}>
              <div style={{ width: 8, height: 8, borderRadius: 2, background: b.color, opacity: 0.8, marginTop: 3 }} />
              <div>
                <div style={{ fontSize: 10, color: C.text, fontWeight: 600 }}>{b.component}</div>
                <div style={{ fontSize: 8, color: C.textDim }}>{b.detail}</div>
              </div>
              <div style={{ fontFamily: MONO, fontSize: 10, color: b.color, fontWeight: 700, textAlign: "right" }}>{fmt(b.gb)}</div>
            </div>
          ))}
        </div>

        {/* Right: Execution pipeline + profile */}
        <div>
          <div style={{ fontSize: 9, fontWeight: 700, color: C.textDim, textTransform: "uppercase", marginBottom: 6, letterSpacing: "0.06em" }}>Execution Pipeline</div>
          {q.pipeline.map((stage, i) => {
            const stageColor = stage.status === "active" ? C.queries : stage.status === "waiting" ? C.warn : C.textDim;
            return (
              <div key={i} style={{ display: "flex", alignItems: "flex-start", gap: 6, marginBottom: 5 }}>
                {/* Connector line */}
                <div style={{ display: "flex", flexDirection: "column", alignItems: "center", width: 16, paddingTop: 2 }}>
                  <div style={{ width: 8, height: 8, borderRadius: "50%", background: stageColor, border: `2px solid ${stageColor}40` }} />
                  {i < q.pipeline.length - 1 && <div style={{ width: 1, height: 18, background: C.border }} />}
                </div>
                <div style={{ flex: 1 }}>
                  <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
                    <span style={{ fontSize: 10, fontWeight: 700, color: stageColor }}>{stage.stage}</span>
                    {stage.threads > 0 && <span style={{ fontSize: 8, fontFamily: MONO, color: C.textMuted, background: C.bgDeep, padding: "0 4px", borderRadius: 2 }}>{stage.threads}T</span>}
                    <span style={{ fontSize: 8, fontFamily: MONO, color: stageColor, opacity: 0.7 }}>{stage.status}</span>
                  </div>
                  <div style={{ fontSize: 8, color: C.textDim, marginTop: 1 }}>{stage.detail}</div>
                </div>
              </div>
            );
          })}

          {/* Key ProfileEvents */}
          <div style={{ marginTop: 8, padding: "6px 8px", background: C.bgDeep, borderRadius: 4, fontSize: 9 }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 3 }}>
              <span style={{ color: C.textDim }}>CPU time (all threads)</span>
              <span style={{ fontFamily: MONO, color: C.text, textAlign: "right" }}>{(totalCpuSec/60).toFixed(1)}m</span>
              <span style={{ color: C.textDim }}>Parallelism factor</span>
              <span style={{ fontFamily: MONO, color: parallelism > 3 ? C.ok : C.warn, textAlign: "right" }}>{parallelism.toFixed(1)}× ({q.threads}/{q.maxThreads} threads)</span>
              <span style={{ color: C.textDim }}>IO wait ratio</span>
              <span style={{ fontFamily: MONO, color: ioWaitPct > 30 ? C.warn : C.text, textAlign: "right" }}>{ioWaitPct.toFixed(0)}%</span>
              <span style={{ color: C.textDim }}>Data scanned</span>
              <span style={{ fontFamily: MONO, color: C.text, textAlign: "right" }}>{fmtBytes(q.profileEvents.ReadCompressedBytes)}</span>
              <span style={{ color: C.textDim }}>Index pruning</span>
              <span style={{ fontFamily: MONO, color: prunedPct > 90 ? C.ok : C.warn, textAlign: "right" }}>{prunedPct.toFixed(0)}% marks skipped ({fmtRate(q.profileEvents.SelectedMarks)}/{fmtRate(q.profileEvents.TotalMarks)})</span>
              <span style={{ color: C.textDim }}>Mark cache hit rate</span>
              <span style={{ fontFamily: MONO, color: markHitRate > 95 ? C.ok : C.warn, textAlign: "right" }}>{markHitRate.toFixed(1)}%</span>
            </div>
          </div>
        </div>
      </div>
    </Card>
  );
}

// --- PK Index Table ---
function PKIndexTable({ tables }) {
  return (
    <div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 80px 60px 70px 70px", gap: 6, padding: "0 4px", marginBottom: 4, fontSize: 8, fontWeight: 700, color: C.textDim, textTransform: "uppercase", letterSpacing: "0.06em" }}>
        <span>Table</span><span style={{ textAlign: "right" }}>PK in RAM</span><span style={{ textAlign: "right" }}>Parts</span><span style={{ textAlign: "right" }}>Rows</span><span style={{ textAlign: "right" }}>Granules</span>
      </div>
      {tables.map((t, i) => (
        <div key={i} style={{
          display: "grid", gridTemplateColumns: "1fr 80px 60px 70px 70px", gap: 6,
          padding: "4px 4px", borderRadius: 3, fontSize: 10,
          background: i % 2 === 0 ? "rgba(255,255,255,0.02)" : "transparent",
          borderLeft: `2px solid ${C.primaryKey}${Math.round(((tables.length - i) / tables.length) * 200 + 55).toString(16).padStart(2,"0")}`,
        }}>
          <span style={{ fontFamily: MONO, color: C.text, fontWeight: 600 }}>{t.table}</span>
          <span style={{ fontFamily: MONO, color: C.primaryKey, fontWeight: 700, textAlign: "right" }}>{fmtBytes(t.pkMem)}</span>
          <span style={{ fontFamily: MONO, color: C.textMuted, textAlign: "right" }}>{t.parts.toLocaleString()}</span>
          <span style={{ fontFamily: MONO, color: C.textMuted, textAlign: "right" }}>{t.rows}</span>
          <span style={{ fontFamily: MONO, color: C.textMuted, textAlign: "right" }}>{fmtRate(t.granules)}</span>
        </div>
      ))}
      <div style={{ fontSize: 8, color: C.textDim, marginTop: 6, padding: "0 4px" }}>
        Sparse index: stores first-row PK values per granule (8192 rows). Always resident in RAM. More parts = more index entries.
      </div>
    </div>
  );
}

// ============================================================
// MAIN COMPONENT
// ============================================================
export default function EngineInternals() {
  const [tick, setTick] = useState(0);
  useEffect(() => { const t = setInterval(() => setTick(p => p + 1), 5000); return () => clearInterval(t); }, []);

  const avgCpuPct = Math.round(CPU_CORES.reduce((s, c) => s + c.pct, 0) / CPU_CORES.length);

  return (
    <div style={{
      fontFamily: "'IBM Plex Sans','SF Pro Text',-apple-system,sans-serif",
      background: C.bg, color: C.text, minHeight: "100vh",
      padding: "16px 20px", maxWidth: 1440, margin: "0 auto",
    }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 14 }}>
        <h1 style={{ fontSize: 18, fontWeight: 700, margin: 0 }}>Engine Internals</h1>
        <span style={{ fontSize: 11, color: C.textDim, fontFamily: MONO, background: C.bgDeep, padding: "3px 10px", borderRadius: 6, border: `1px solid ${C.border}` }}>
          chi-prod-01 · v24.8.4 · 16 cores · {TOTAL_RAM_GB} GB
        </span>
        <div style={{ marginLeft: "auto", fontSize: 10, color: C.textDim }}>
          polled 2s ago · <span style={{ color: C.ok }}>●</span> healthy
        </div>
      </div>

      {/* ===== SECTION 1: MEMORY X-RAY ===== */}
      <Card style={{ marginBottom: 12 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 2 }}>
          <Label sub={`RSS ${fmt(MEMORY_MAP.totalRSS)} / ${TOTAL_RAM_GB} GB · jemalloc allocated ${fmt(MEMORY_MAP.jemalloc.allocated)} · retained ${fmt(MEMORY_MAP.jemalloc.retained)}`}>
            Memory X-Ray
          </Label>
          <div style={{ fontFamily: MONO, fontSize: 18, fontWeight: 800, color: MEMORY_MAP.totalRSS / TOTAL_RAM_GB > 0.85 ? C.crit : MEMORY_MAP.totalRSS / TOTAL_RAM_GB > 0.7 ? C.warn : C.text }}>
            {pct(MEMORY_MAP.totalRSS, TOTAL_RAM_GB)}%
          </div>
        </div>
        <MemoryWaterfall subsystems={MEMORY_MAP.subsystems} totalRAM={TOTAL_RAM_GB} />
      </Card>

      {/* ===== SECTION 2: CPU + THREADS ===== */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 320px", gap: 12, marginBottom: 12 }}>
        <Card>
          <Label sub={`${avgCpuPct}% avg across ${CPU_CORES.length} cores · each cell shows % utilization + owner`}>
            CPU Core Map
          </Label>
          <CoreHeatmap cores={CPU_CORES} />
        </Card>

        <Card>
          <Label sub="Active / Max capacity per pool">Thread Pools</Label>
          <ThreadPoolViz pools={THREAD_POOLS} />
          <div style={{ marginTop: 12, padding: "8px", background: C.bgDeep, borderRadius: 5, fontSize: 9, color: C.textDim }}>
            Query threads execute pipeline operators (Scan → Filter → Aggregate → Sort). Merge threads run k-way merge sort on parts. Saturation = queueing.
          </div>
        </Card>
      </div>

      {/* ===== SECTION 3: PK INDEX + DICTIONARIES ===== */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 12 }}>
        <Card>
          <Label sub={`${fmt(PK_BY_TABLE.reduce((s,t)=>s+t.pkMem,0)/1e9)} total · always resident in RAM`}>
            Primary Key Index (by table)
          </Label>
          <PKIndexTable tables={PK_BY_TABLE} />
        </Card>

        <Card>
          <Label sub={`${MEMORY_MAP.subsystems.find(s=>s.id==="dictionaries").sub.count} dictionaries · ${fmt(MEMORY_MAP.subsystems.find(s=>s.id==="dictionaries").gb)} RAM`}>
            Dictionaries in Memory
          </Label>
          {MEMORY_MAP.subsystems.find(s => s.id === "dictionaries").sub.names.map((name, i) => (
            <div key={i} style={{
              display: "flex", alignItems: "center", gap: 8, padding: "6px 8px",
              borderRadius: 4, marginBottom: 3,
              background: "rgba(251,147,60,0.06)", borderLeft: `3px solid ${C.dictionaries}`,
            }}>
              <span style={{ fontFamily: MONO, fontSize: 11, color: C.dictionaries, fontWeight: 600 }}>{name}</span>
              <span style={{ fontSize: 9, color: C.textDim, marginLeft: "auto" }}>hashed · loaded</span>
            </div>
          ))}
          <div style={{ marginTop: 12, fontSize: 9, fontWeight: 700, color: C.textDim, textTransform: "uppercase", letterSpacing: "0.06em", marginBottom: 6 }}>
            jemalloc Arena Summary
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 3, fontSize: 10, padding: "6px 8px", background: C.bgDeep, borderRadius: 4 }}>
            {[
              ["Allocated", fmt(MEMORY_MAP.jemalloc.allocated)],
              ["Resident", fmt(MEMORY_MAP.jemalloc.resident)],
              ["Mapped", fmt(MEMORY_MAP.jemalloc.mapped)],
              ["Retained", fmt(MEMORY_MAP.jemalloc.retained)],
              ["Metadata", fmt(MEMORY_MAP.jemalloc.metadata)],
              ["Fragmentation", ((1 - MEMORY_MAP.jemalloc.allocated / MEMORY_MAP.jemalloc.resident) * 100).toFixed(1) + "%"],
            ].map(([k, v], i) => (
              <div key={i} style={{ display: "flex", justifyContent: "space-between", color: C.textMuted }}>
                <span>{k}</span>
                <span style={{ fontFamily: MONO, color: C.text }}>{v}</span>
              </div>
            ))}
          </div>
        </Card>
      </div>

      {/* ===== SECTION 4: QUERY INTERNALS ===== */}
      <div style={{ marginBottom: 12 }}>
        <Label sub={`${QUERY_INTERNALS.length} running queries — showing internal pipeline stages, memory anatomy, and ProfileEvents`}>
          Query Pipeline X-Ray
        </Label>
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          {QUERY_INTERNALS.map(q => <QueryInternalsCard key={q.id} q={q} />)}
        </div>
      </div>

      {/* ===== FOOTER ===== */}
      <div style={{ padding: "10px 0", fontSize: 9, color: C.textDim, borderTop: `1px solid ${C.border}`, lineHeight: 1.7 }}>
        <strong style={{ color: C.textMuted }}>Data sources:</strong> Memory from <code style={{ color: C.textMuted }}>jemalloc.*</code> + <code style={{ color: C.textMuted }}>MarkCacheBytes</code>/<code style={{ color: C.textMuted }}>UncompressedCacheBytes</code> in <code style={{ color: C.textMuted }}>system.asynchronous_metrics</code>,
        PK from <code style={{ color: C.textMuted }}>primary_key_bytes_in_memory</code> in <code style={{ color: C.textMuted }}>system.parts</code>,
        dictionaries from <code style={{ color: C.textMuted }}>system.dictionaries</code>,
        per-query breakdown from <code style={{ color: C.textMuted }}>system.processes</code> ProfileEvents (UserTimeMicroseconds, SelectedMarks, MarkCacheHits, etc.),
        CPU per-core from <code style={{ color: C.textMuted }}>OSUserTime_N</code>/<code style={{ color: C.textMuted }}>OSSystemTime_N</code>/<code style={{ color: C.textMuted }}>OSIOWaitTime_N</code> in async metrics,
        thread pools from <code style={{ color: C.textMuted }}>system.metrics</code>.
        Pipeline stages reconstructed from <code style={{ color: C.textMuted }}>EXPLAIN PIPELINE</code> + live ProfileEvents.
      </div>
    </div>
  );
}