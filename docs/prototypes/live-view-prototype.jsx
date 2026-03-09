import { useState, useEffect, useRef } from "react";

// ============================================================
// MOCK DATA — replace with real ClickHouse polling
// ============================================================

const MOCK_SERVER = {
  hostname: "chi-prod-01",
  version: "24.8.4.13",
  uptime: "14d 7h 23m",
  cores: 16,
  totalRAM: 64 * 1024 * 1024 * 1024,
  lastPoll: "2s ago",
};

const MOCK_CPU = {
  totalPct: 68.4, // total process CPU as % of all cores
  cores: 16,
  breakdown: {
    queries: 41.2,
    merges: 19.8,
    mutations: 3.1,
    other: 4.3,
  },
};

const MOCK_MEMORY = {
  totalRSS: 48.2, // GB
  totalRAM: 64,
  tracked: 31.4,
  breakdown: {
    queries: 22.1,
    merges: 4.8,
    markCache: 6.2,
    uncompressedCache: 2.1,
    primaryKeys: 3.4,
    dictionaries: 1.1,
    other: 8.5,
  },
};

const MOCK_IO = {
  readMBs: 847,
  writeMBs: 312,
  breakdown: {
    queryRead: 520,
    queryWrite: 45,
    mergeRead: 280,
    mergeWrite: 260,
    replicationRead: 47,
    replicationWrite: 7,
  },
};

const MOCK_QUERIES = [
  { id: "3f60b9d4", user: "analytics", elapsed: 441.5, cpuCores: 4.2, cpuPct: 26.3, mem: 12.4e9, memPct: 25.7, ioRead: 280, progress: 67, kind: "SELECT", query: "SELECT a.country_code, b.device_type, count() as combinations FROM events a JOIN devices b ON a.device_id = b.id GROUP BY 1, 2 ORDER BY 3 DESC", status: "running", rows: "2.1B", bytesRead: "148 GB" },
  { id: "83e4b0dc", user: "default", elapsed: 87.2, cpuCores: 2.1, cpuPct: 13.1, mem: 4.2e9, memPct: 8.7, ioRead: 120, progress: 34, kind: "SELECT", query: "SELECT toStartOfHour(timestamp) AS hour, uniqExact(user_id) AS unique_users FROM pageviews WHERE date >= today() - 7 GROUP BY hour", status: "running", rows: "890M", bytesRead: "34 GB" },
  { id: "bfc0a86c", user: "etl_user", elapsed: 12.3, cpuCores: 0.8, cpuPct: 5.0, mem: 1.8e9, memPct: 3.7, ioRead: 65, progress: 89, kind: "INSERT", query: "INSERT INTO events_daily SELECT toDate(timestamp), count(), uniqExact(user_id) FROM events WHERE date = yesterday() GROUP BY 1", status: "running", rows: "45M", bytesRead: "2.1 GB" },
  { id: "c5a31f39", user: "analytics", elapsed: 5.1, cpuCores: 0.3, cpuPct: 1.9, mem: 0.4e9, memPct: 0.8, ioRead: 15, progress: 12, kind: "SELECT", query: "SELECT count() FROM events WHERE date = today() AND country_code = 'US'", status: "running", rows: "12M", bytesRead: "0.8 GB" },
];

const MOCK_MERGES = [
  { table: "events", partName: "202602_1_1847_312", elapsed: 234, progress: 72, mem: 2.1e9, memPct: 4.4, readMBs: 145, writeMBs: 132, rows: "48M", numParts: 12, cpuEst: 1.8 },
  { table: "pageviews", partName: "202602_445_892_6", elapsed: 45, progress: 34, mem: 0.8e9, memPct: 1.7, readMBs: 67, writeMBs: 58, rows: "12M", numParts: 6, cpuEst: 0.9 },
  { table: "system.metric_log", partName: "20260215_3346_3646_60", elapsed: 1.1, progress: 89, mem: 0.12e9, memPct: 0.2, readMBs: 34, writeMBs: 28, rows: "1.2M", numParts: 3, cpuEst: 0.4 },
  { table: "system.trace_log", partName: "20260215_3327_3650_7", elapsed: 0.5, progress: 45, mem: 0.08e9, memPct: 0.2, readMBs: 22, writeMBs: 18, rows: "0.8M", numParts: 4, cpuEst: 0.3 },
];

const MOCK_MUTATIONS = [
  { table: "events_old", command: "ALTER TABLE DELETE WHERE date < '2025-01-01'", partsToDo: 23, elapsed: 1847, mem: 1.2e9, cpuEst: 0.6, status: "working" },
];

const MOCK_REPLICATION = {
  totalTables: 14,
  healthyTables: 13,
  readonlyReplicas: 0,
  maxDelay: 2,
  queueSize: 3,
  fetchesActive: 1,
};

const MOCK_ALERTS = [
  { severity: "warn", message: "events: 187 parts in partition 202602 (limit: 300)", source: "parts" },
];

const MOCK_DISK = { usedPct: 72, totalTB: 2.0, freeTB: 0.56 };

// ============================================================
// UTILITY FUNCTIONS
// ============================================================

function formatBytes(bytes) {
  if (bytes >= 1e12) return (bytes / 1e12).toFixed(1) + " TB";
  if (bytes >= 1e9) return (bytes / 1e9).toFixed(1) + " GB";
  if (bytes >= 1e6) return (bytes / 1e6).toFixed(1) + " MB";
  if (bytes >= 1e3) return (bytes / 1e3).toFixed(0) + " KB";
  return bytes + " B";
}

function formatDuration(sec) {
  if (sec >= 3600) return Math.floor(sec / 3600) + "h " + Math.floor((sec % 3600) / 60) + "m";
  if (sec >= 60) return Math.floor(sec / 60) + "m " + Math.floor(sec % 60) + "s";
  return sec.toFixed(1) + "s";
}

function formatRate(mbPerSec) {
  if (mbPerSec >= 1000) return (mbPerSec / 1000).toFixed(1) + " GB/s";
  return mbPerSec.toFixed(0) + " MB/s";
}

// ============================================================
// COLOR SYSTEM
// ============================================================

const COLORS = {
  queries: "#3b82f6",    // blue
  merges: "#f59e0b",     // amber
  mutations: "#ef4444",  // red
  replication: "#8b5cf6",// purple
  mvs: "#06b6d4",       // cyan
  other: "#94a3b8",      // slate
  cache: "#10b981",      // emerald (caches, pk, dicts)
  bg: "#0f172a",         // slate-900
  bgCard: "#1e293b",     // slate-800
  bgCardHover: "#334155", // slate-700
  border: "#334155",
  text: "#e2e8f0",       // slate-200
  textMuted: "#94a3b8",  // slate-400
  textDim: "#64748b",    // slate-500
  ok: "#22c55e",
  warn: "#f59e0b",
  crit: "#ef4444",
};

// ============================================================
// COMPONENTS
// ============================================================

// --- Horizontal stacked bar (the core visual) ---
function AttributionBar({ segments, height = 32, showLabels = true, totalLabel = "" }) {
  const total = segments.reduce((s, seg) => s + seg.value, 0);
  return (
    <div style={{ width: "100%" }}>
      {totalLabel && (
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4, fontSize: 11, color: COLORS.textMuted }}>
          <span>{totalLabel}</span>
          <span>{total.toFixed(1)}%</span>
        </div>
      )}
      <div style={{
        display: "flex", height, borderRadius: 6, overflow: "hidden",
        background: "#0f172a", border: `1px solid ${COLORS.border}`,
        position: "relative",
      }}>
        {segments.map((seg, i) => {
          const widthPct = total > 0 ? (seg.value / 100) * 100 : 0;
          if (widthPct < 0.5) return null;
          return (
            <div key={i} title={`${seg.label}: ${seg.value.toFixed(1)}%`} style={{
              width: widthPct + "%",
              background: seg.color,
              opacity: 0.85,
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 10, fontWeight: 600, color: "#fff",
              letterSpacing: "0.02em",
              transition: "width 0.6s ease",
              whiteSpace: "nowrap", overflow: "hidden",
              borderRight: i < segments.length - 1 ? "1px solid rgba(0,0,0,0.3)" : "none",
            }}>
              {widthPct > 8 && showLabels ? `${seg.label} ${seg.value.toFixed(0)}%` : widthPct > 4 ? `${seg.value.toFixed(0)}%` : ""}
            </div>
          );
        })}
        {/* Unused portion */}
        <div style={{ flex: 1, background: "transparent" }} />
      </div>
      {showLabels && (
        <div style={{ display: "flex", gap: 12, marginTop: 6, flexWrap: "wrap" }}>
          {segments.filter(s => s.value > 0.5).map((seg, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 4, fontSize: 10, color: COLORS.textMuted }}>
              <div style={{ width: 8, height: 8, borderRadius: 2, background: seg.color, opacity: 0.85 }} />
              {seg.label}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// --- Mini spark-style bar for table cells ---
function MiniBar({ value, max, color, width = 60 }) {
  const pct = Math.min(100, (value / max) * 100);
  return (
    <div style={{ width, height: 6, background: "#1e293b", borderRadius: 3, overflow: "hidden" }}>
      <div style={{
        width: pct + "%", height: "100%", background: color, borderRadius: 3,
        transition: "width 0.4s ease",
      }} />
    </div>
  );
}

// --- Progress ring for compact display ---
function ProgressRing({ pct, size = 44, stroke = 4, color, label }) {
  const r = (size - stroke) / 2;
  const circ = 2 * Math.PI * r;
  const offset = circ * (1 - Math.min(pct, 100) / 100);
  return (
    <div style={{ position: "relative", width: size, height: size }}>
      <svg width={size} height={size} style={{ transform: "rotate(-90deg)" }}>
        <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke={COLORS.border} strokeWidth={stroke} />
        <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke={color} strokeWidth={stroke}
          strokeDasharray={circ} strokeDashoffset={offset} strokeLinecap="round"
          style={{ transition: "stroke-dashoffset 0.6s ease" }} />
      </svg>
      <div style={{
        position: "absolute", inset: 0, display: "flex", flexDirection: "column",
        alignItems: "center", justifyContent: "center",
        fontSize: 11, fontWeight: 700, color: COLORS.text, lineHeight: 1.1,
      }}>
        {pct.toFixed(0)}%
      </div>
    </div>
  );
}

// --- Status dot ---
function StatusDot({ status }) {
  const color = status === "ok" ? COLORS.ok : status === "warn" ? COLORS.warn : COLORS.crit;
  return (
    <span style={{
      display: "inline-block", width: 7, height: 7, borderRadius: "50%",
      background: color, boxShadow: `0 0 6px ${color}60`,
    }} />
  );
}

// --- Card container ---
function Card({ children, style = {} }) {
  return (
    <div style={{
      background: COLORS.bgCard,
      borderRadius: 10,
      border: `1px solid ${COLORS.border}`,
      padding: "14px 16px",
      ...style,
    }}>
      {children}
    </div>
  );
}

function SectionTitle({ children, count, icon }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 10 }}>
      {icon && <span style={{ fontSize: 14, opacity: 0.7 }}>{icon}</span>}
      <span style={{ fontSize: 13, fontWeight: 700, color: COLORS.text, letterSpacing: "0.03em", textTransform: "uppercase" }}>{children}</span>
      {count !== undefined && (
        <span style={{
          fontSize: 11, fontWeight: 600, color: COLORS.textMuted,
          background: "#0f172a", padding: "1px 7px", borderRadius: 8,
        }}>{count}</span>
      )}
    </div>
  );
}

// --- The Query/Merge/Mutation row component ---
function ActivityRow({ item, type, maxCpu, maxMem }) {
  const color = type === "query" ? COLORS.queries : type === "merge" ? COLORS.merges : COLORS.mutations;
  const cpuVal = type === "query" ? item.cpuCores : item.cpuEst;
  const memVal = type === "query" ? item.mem : item.mem;

  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: type === "query"
        ? "minmax(0,2.8fr) 70px 80px 54px 54px 80px 50px"
        : "minmax(0,2.8fr) 70px 80px 54px 54px 80px 50px",
      gap: 8, alignItems: "center",
      padding: "7px 8px", borderRadius: 6,
      fontSize: 12, color: COLORS.text,
      borderLeft: `3px solid ${color}`,
      background: "rgba(255,255,255,0.02)",
      marginBottom: 2,
      cursor: "pointer",
      transition: "background 0.15s",
    }}
    onMouseEnter={e => e.currentTarget.style.background = "rgba(255,255,255,0.06)"}
    onMouseLeave={e => e.currentTarget.style.background = "rgba(255,255,255,0.02)"}
    >
      {/* Identity */}
      <div style={{ overflow: "hidden" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
          <span style={{ fontFamily: "'JetBrains Mono', 'SF Mono', 'Fira Code', monospace", fontSize: 11, color: color, fontWeight: 600 }}>
            {type === "query" ? item.id : item.table}
          </span>
          <span style={{
            fontSize: 9, fontWeight: 600, padding: "1px 5px", borderRadius: 3,
            background: color + "20", color: color, textTransform: "uppercase",
          }}>
            {type === "query" ? item.kind : type === "merge" ? `${item.numParts}→1` : "mut"}
          </span>
          {type === "query" && (
            <span style={{ fontSize: 10, color: COLORS.textDim }}>{item.user}</span>
          )}
        </div>
        <div style={{
          fontSize: 10, color: COLORS.textDim, marginTop: 2,
          fontFamily: "'JetBrains Mono', 'SF Mono', monospace",
          overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
        }}>
          {type === "query" ? item.query.substring(0, 90) + "…" : item.partName}
        </div>
      </div>

      {/* Duration */}
      <div style={{ textAlign: "right", fontFamily: "monospace", fontSize: 11 }}>
        {formatDuration(item.elapsed)}
      </div>

      {/* CPU cores */}
      <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
        <MiniBar value={cpuVal} max={maxCpu} color={color} width={40} />
        <span style={{ fontFamily: "monospace", fontSize: 11, fontWeight: 600, color: color, minWidth: 30, textAlign: "right" }}>
          {cpuVal.toFixed(1)}c
        </span>
      </div>

      {/* Memory */}
      <div style={{ fontFamily: "monospace", fontSize: 11, textAlign: "right", color: COLORS.textMuted }}>
        {formatBytes(memVal)}
      </div>

      {/* IO read */}
      <div style={{ fontFamily: "monospace", fontSize: 11, textAlign: "right", color: COLORS.textMuted }}>
        {type === "query" ? formatRate(item.ioRead) : formatRate(item.readMBs)}
      </div>

      {/* Rows/Bytes */}
      <div style={{ fontFamily: "monospace", fontSize: 10, textAlign: "right", color: COLORS.textDim }}>
        {type === "query" ? item.rows + " rows" : item.rows + " rows"}
      </div>

      {/* Progress */}
      <div>
        <ProgressRing pct={type === "query" ? item.progress : item.progress} size={32} stroke={3} color={color} />
      </div>
    </div>
  );
}

// --- Alert banner ---
function AlertBanner({ alerts }) {
  if (!alerts || alerts.length === 0) return null;
  return (
    <div style={{
      display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 12,
    }}>
      {alerts.map((a, i) => (
        <div key={i} style={{
          display: "flex", alignItems: "center", gap: 6,
          padding: "5px 12px", borderRadius: 6, fontSize: 11, fontWeight: 500,
          background: a.severity === "crit" ? "#ef444420" : "#f59e0b18",
          border: `1px solid ${a.severity === "crit" ? "#ef444450" : "#f59e0b40"}`,
          color: a.severity === "crit" ? "#fca5a5" : "#fcd34d",
        }}>
          <span>{a.severity === "crit" ? "🔴" : "⚠️"}</span>
          {a.message}
        </div>
      ))}
    </div>
  );
}

// ============================================================
// MAIN DASHBOARD
// ============================================================

export default function ClickHouseLiveView() {
  const [tick, setTick] = useState(0);
  const [selectedResource, setSelectedResource] = useState("cpu");

  // Simulated polling tick
  useEffect(() => {
    const t = setInterval(() => setTick(p => p + 1), 5000);
    return () => clearInterval(t);
  }, []);

  const maxQueryCpu = Math.max(...MOCK_QUERIES.map(q => q.cpuCores), 1);
  const maxMergeCpu = Math.max(...MOCK_MERGES.map(m => m.cpuEst), 1);
  const maxCpuAll = Math.max(maxQueryCpu, maxMergeCpu, 1);
  const maxMem = Math.max(...MOCK_QUERIES.map(q => q.mem), ...MOCK_MERGES.map(m => m.mem), 1);

  // Build attribution segments based on selected resource
  const cpuSegments = [
    { label: "Queries", value: MOCK_CPU.breakdown.queries, color: COLORS.queries },
    { label: "Merges", value: MOCK_CPU.breakdown.merges, color: COLORS.merges },
    { label: "Mutations", value: MOCK_CPU.breakdown.mutations, color: COLORS.mutations },
    { label: "Other", value: MOCK_CPU.breakdown.other, color: COLORS.other },
  ];

  const memSegments = [
    { label: "Queries", value: (MOCK_MEMORY.breakdown.queries / MOCK_MEMORY.totalRAM) * 100, color: COLORS.queries },
    { label: "Merges", value: (MOCK_MEMORY.breakdown.merges / MOCK_MEMORY.totalRAM) * 100, color: COLORS.merges },
    { label: "Mark Cache", value: (MOCK_MEMORY.breakdown.markCache / MOCK_MEMORY.totalRAM) * 100, color: COLORS.cache },
    { label: "Uncomp Cache", value: (MOCK_MEMORY.breakdown.uncompressedCache / MOCK_MEMORY.totalRAM) * 100, color: "#14b8a6" },
    { label: "Primary Keys", value: (MOCK_MEMORY.breakdown.primaryKeys / MOCK_MEMORY.totalRAM) * 100, color: "#a78bfa" },
    { label: "Dictionaries", value: (MOCK_MEMORY.breakdown.dictionaries / MOCK_MEMORY.totalRAM) * 100, color: "#fb923c" },
    { label: "Other", value: (MOCK_MEMORY.breakdown.other / MOCK_MEMORY.totalRAM) * 100, color: COLORS.other },
  ];

  const ioSegments = [
    { label: "Query Read", value: (MOCK_IO.breakdown.queryRead / (MOCK_IO.readMBs + MOCK_IO.writeMBs)) * 100, color: COLORS.queries },
    { label: "Query Write", value: (MOCK_IO.breakdown.queryWrite / (MOCK_IO.readMBs + MOCK_IO.writeMBs)) * 100, color: "#60a5fa" },
    { label: "Merge Read", value: (MOCK_IO.breakdown.mergeRead / (MOCK_IO.readMBs + MOCK_IO.writeMBs)) * 100, color: COLORS.merges },
    { label: "Merge Write", value: (MOCK_IO.breakdown.mergeWrite / (MOCK_IO.readMBs + MOCK_IO.writeMBs)) * 100, color: "#fbbf24" },
    { label: "Replication", value: ((MOCK_IO.breakdown.replicationRead + MOCK_IO.breakdown.replicationWrite) / (MOCK_IO.readMBs + MOCK_IO.writeMBs)) * 100, color: COLORS.replication },
  ];

  const activeSegments = selectedResource === "cpu" ? cpuSegments : selectedResource === "memory" ? memSegments : ioSegments;
  const activeTotal = selectedResource === "cpu"
    ? `${MOCK_CPU.totalPct.toFixed(0)}% of ${MOCK_CPU.cores} cores (${(MOCK_CPU.totalPct * MOCK_CPU.cores / 100).toFixed(1)} cores used)`
    : selectedResource === "memory"
      ? `${MOCK_MEMORY.totalRSS.toFixed(1)} GB of ${MOCK_MEMORY.totalRAM} GB RAM (${(MOCK_MEMORY.totalRSS / MOCK_MEMORY.totalRAM * 100).toFixed(0)}%)`
      : `${formatRate(MOCK_IO.readMBs)} read · ${formatRate(MOCK_IO.writeMBs)} write`;

  return (
    <div style={{
      fontFamily: "'IBM Plex Sans', 'SF Pro Text', -apple-system, sans-serif",
      background: COLORS.bg,
      color: COLORS.text,
      minHeight: "100vh",
      padding: "16px 20px",
      maxWidth: 1440,
      margin: "0 auto",
    }}>
      {/* ===== HEADER ===== */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <h1 style={{ fontSize: 18, fontWeight: 700, margin: 0, letterSpacing: "-0.02em" }}>
            Live View
          </h1>
          <div style={{
            display: "flex", alignItems: "center", gap: 6,
            fontSize: 11, color: COLORS.textMuted, fontFamily: "monospace",
            background: "#0f172a", padding: "3px 10px", borderRadius: 6,
            border: `1px solid ${COLORS.border}`,
          }}>
            <StatusDot status="ok" />
            {MOCK_SERVER.hostname}
            <span style={{ color: COLORS.textDim }}>·</span>
            v{MOCK_SERVER.version}
            <span style={{ color: COLORS.textDim }}>·</span>
            up {MOCK_SERVER.uptime}
          </div>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          {/* Summary pills */}
          {[
            { label: "Queries", value: MOCK_QUERIES.length, color: COLORS.queries },
            { label: "Merges", value: MOCK_MERGES.length, color: COLORS.merges },
            { label: "Mutations", value: MOCK_MUTATIONS.length, color: MOCK_MUTATIONS.length > 0 ? COLORS.mutations : COLORS.textDim },
            { label: "Replication", value: `${MOCK_REPLICATION.maxDelay}s lag`, color: MOCK_REPLICATION.readonlyReplicas > 0 ? COLORS.crit : COLORS.ok },
          ].map((pill, i) => (
            <div key={i} style={{
              display: "flex", alignItems: "center", gap: 5,
              fontSize: 11, fontWeight: 600, padding: "4px 10px", borderRadius: 6,
              background: pill.color + "15",
              border: `1px solid ${pill.color}30`,
              color: pill.color,
            }}>
              {pill.label}
              <span style={{ fontFamily: "monospace", fontWeight: 700 }}>{pill.value}</span>
            </div>
          ))}

          <div style={{
            fontSize: 10, color: COLORS.textDim, marginLeft: 4,
            fontFamily: "monospace",
          }}>
            polled {MOCK_SERVER.lastPoll}
          </div>
        </div>
      </div>

      {/* ===== ALERTS ===== */}
      <AlertBanner alerts={MOCK_ALERTS} />

      {/* ===== RESOURCE ATTRIBUTION (the hero section) ===== */}
      <Card style={{ marginBottom: 14 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 10 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 4 }}>
            <SectionTitle icon="📊">Resource Attribution</SectionTitle>
          </div>

          {/* Resource selector tabs */}
          <div style={{ display: "flex", gap: 2, background: "#0f172a", borderRadius: 6, padding: 2 }}>
            {[
              { key: "cpu", label: "CPU", value: `${MOCK_CPU.totalPct.toFixed(0)}%` },
              { key: "memory", label: "Memory", value: `${(MOCK_MEMORY.totalRSS / MOCK_MEMORY.totalRAM * 100).toFixed(0)}%` },
              { key: "io", label: "Disk I/O", value: formatRate(MOCK_IO.readMBs + MOCK_IO.writeMBs) },
            ].map(tab => (
              <button key={tab.key} onClick={() => setSelectedResource(tab.key)} style={{
                padding: "5px 14px", borderRadius: 5, border: "none", cursor: "pointer",
                fontSize: 11, fontWeight: 600, transition: "all 0.15s",
                background: selectedResource === tab.key ? COLORS.bgCardHover : "transparent",
                color: selectedResource === tab.key ? COLORS.text : COLORS.textDim,
              }}>
                {tab.label} <span style={{ fontFamily: "monospace", fontWeight: 700, marginLeft: 3 }}>{tab.value}</span>
              </button>
            ))}
          </div>
        </div>

        {/* The attribution bar — THE KEY VISUAL */}
        <AttributionBar
          segments={activeSegments}
          height={36}
          totalLabel={activeTotal}
        />
      </Card>

      {/* ===== 3-COLUMN SUMMARY ROW ===== */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 10, marginBottom: 14 }}>
        {/* CPU summary */}
        <Card>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <div>
              <div style={{ fontSize: 10, color: COLORS.textDim, textTransform: "uppercase", fontWeight: 600, letterSpacing: "0.05em" }}>CPU</div>
              <div style={{ fontSize: 24, fontWeight: 700, fontFamily: "monospace", lineHeight: 1.2 }}>
                {(MOCK_CPU.totalPct * MOCK_CPU.cores / 100).toFixed(1)}
                <span style={{ fontSize: 13, color: COLORS.textMuted, fontWeight: 500 }}> / {MOCK_CPU.cores} cores</span>
              </div>
            </div>
            <ProgressRing pct={MOCK_CPU.totalPct} size={48} stroke={4}
              color={MOCK_CPU.totalPct > 80 ? COLORS.crit : MOCK_CPU.totalPct > 60 ? COLORS.warn : COLORS.ok} />
          </div>
          <div style={{ marginTop: 8, display: "flex", gap: 12, fontSize: 10, color: COLORS.textMuted }}>
            <span>Load <span style={{ fontFamily: "monospace", color: COLORS.text }}>5.2 · 4.8 · 3.1</span></span>
          </div>
        </Card>

        {/* Memory summary */}
        <Card>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <div>
              <div style={{ fontSize: 10, color: COLORS.textDim, textTransform: "uppercase", fontWeight: 600, letterSpacing: "0.05em" }}>Memory</div>
              <div style={{ fontSize: 24, fontWeight: 700, fontFamily: "monospace", lineHeight: 1.2 }}>
                {MOCK_MEMORY.totalRSS.toFixed(1)}
                <span style={{ fontSize: 13, color: COLORS.textMuted, fontWeight: 500 }}> / {MOCK_MEMORY.totalRAM} GB</span>
              </div>
            </div>
            <ProgressRing pct={(MOCK_MEMORY.totalRSS / MOCK_MEMORY.totalRAM) * 100} size={48} stroke={4}
              color={MOCK_MEMORY.totalRSS / MOCK_MEMORY.totalRAM > 0.85 ? COLORS.crit : MOCK_MEMORY.totalRSS / MOCK_MEMORY.totalRAM > 0.7 ? COLORS.warn : COLORS.ok} />
          </div>
          <div style={{ marginTop: 8, display: "flex", gap: 12, fontSize: 10, color: COLORS.textMuted }}>
            <span>Tracked <span style={{ fontFamily: "monospace", color: COLORS.text }}>{MOCK_MEMORY.tracked} GB</span></span>
            <span>Caches <span style={{ fontFamily: "monospace", color: COLORS.text }}>{(MOCK_MEMORY.breakdown.markCache + MOCK_MEMORY.breakdown.uncompressedCache).toFixed(1)} GB</span></span>
          </div>
        </Card>

        {/* Disk / Replication summary */}
        <Card>
          <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
            <div>
              <div style={{ fontSize: 10, color: COLORS.textDim, textTransform: "uppercase", fontWeight: 600, letterSpacing: "0.05em" }}>Disk / Replication</div>
              <div style={{ fontSize: 24, fontWeight: 700, fontFamily: "monospace", lineHeight: 1.2 }}>
                {MOCK_DISK.usedPct}%
                <span style={{ fontSize: 13, color: COLORS.textMuted, fontWeight: 500 }}> used</span>
              </div>
            </div>
            <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 2 }}>
              <ProgressRing pct={MOCK_DISK.usedPct} size={48} stroke={4}
                color={MOCK_DISK.usedPct > 85 ? COLORS.crit : MOCK_DISK.usedPct > 70 ? COLORS.warn : COLORS.ok} />
            </div>
          </div>
          <div style={{ marginTop: 8, display: "flex", gap: 12, fontSize: 10, color: COLORS.textMuted }}>
            <span>Free <span style={{ fontFamily: "monospace", color: COLORS.text }}>{MOCK_DISK.freeTB} TB</span></span>
            <span>Repl <span style={{ fontFamily: "monospace", color: COLORS.text }}>{MOCK_REPLICATION.healthyTables}/{MOCK_REPLICATION.totalTables}</span> tables</span>
            {MOCK_REPLICATION.readonlyReplicas > 0 && (
              <span style={{ color: COLORS.crit, fontWeight: 600 }}>⚠ {MOCK_REPLICATION.readonlyReplicas} readonly!</span>
            )}
          </div>
        </Card>
      </div>

      {/* ===== ACTIVITY TABLES ===== */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 12 }}>

        {/* --- QUERIES --- */}
        <Card>
          <SectionTitle icon="⚡" count={MOCK_QUERIES.length}>Running Queries</SectionTitle>

          {/* Column headers */}
          <div style={{
            display: "grid",
            gridTemplateColumns: "minmax(0,2.8fr) 70px 80px 54px 54px 80px 50px",
            gap: 8, padding: "0 8px", marginBottom: 4,
            fontSize: 9, fontWeight: 600, color: COLORS.textDim, textTransform: "uppercase", letterSpacing: "0.06em",
          }}>
            <span>Query</span>
            <span style={{ textAlign: "right" }}>Elapsed</span>
            <span>CPU Cores</span>
            <span style={{ textAlign: "right" }}>Memory</span>
            <span style={{ textAlign: "right" }}>IO</span>
            <span style={{ textAlign: "right" }}>Rows</span>
            <span style={{ textAlign: "center" }}>Prog</span>
          </div>

          {MOCK_QUERIES.map(q => (
            <ActivityRow key={q.id} item={q} type="query" maxCpu={maxCpuAll} maxMem={maxMem} />
          ))}
        </Card>

        {/* --- MERGES + MUTATIONS side by side --- */}
        <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
          <Card>
            <SectionTitle icon="🔀" count={MOCK_MERGES.length}>Active Merges</SectionTitle>

            <div style={{
              display: "grid",
              gridTemplateColumns: "minmax(0,2.8fr) 70px 80px 54px 54px 80px 50px",
              gap: 8, padding: "0 8px", marginBottom: 4,
              fontSize: 9, fontWeight: 600, color: COLORS.textDim, textTransform: "uppercase", letterSpacing: "0.06em",
            }}>
              <span>Table / Part</span>
              <span style={{ textAlign: "right" }}>Elapsed</span>
              <span>CPU Est</span>
              <span style={{ textAlign: "right" }}>Memory</span>
              <span style={{ textAlign: "right" }}>IO</span>
              <span style={{ textAlign: "right" }}>Rows</span>
              <span style={{ textAlign: "center" }}>Prog</span>
            </div>

            {MOCK_MERGES.map((m, i) => (
              <ActivityRow key={i} item={m} type="merge" maxCpu={maxCpuAll} maxMem={maxMem} />
            ))}
          </Card>

          <Card>
            <SectionTitle icon="✏️" count={MOCK_MUTATIONS.length}>Active Mutations</SectionTitle>

            {MOCK_MUTATIONS.map((m, i) => (
              <div key={i} style={{
                padding: "10px 10px",
                borderLeft: `3px solid ${COLORS.mutations}`,
                background: "rgba(239,68,68,0.04)",
                borderRadius: 6, marginBottom: 4,
              }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <span style={{ fontFamily: "monospace", fontSize: 12, color: COLORS.mutations, fontWeight: 600 }}>{m.table}</span>
                  <span style={{ fontFamily: "monospace", fontSize: 11, color: COLORS.textMuted }}>{formatDuration(m.elapsed)}</span>
                </div>
                <div style={{ fontSize: 10, color: COLORS.textDim, fontFamily: "monospace", marginTop: 4 }}>
                  {m.command.substring(0, 60)}…
                </div>
                <div style={{ display: "flex", gap: 16, marginTop: 8, fontSize: 10, color: COLORS.textMuted }}>
                  <span>Parts remaining: <span style={{ fontFamily: "monospace", color: m.partsToDo > 10 ? COLORS.warn : COLORS.text, fontWeight: 600 }}>{m.partsToDo}</span></span>
                  <span>Memory: <span style={{ fontFamily: "monospace" }}>{formatBytes(m.mem)}</span></span>
                  <span>CPU est: <span style={{ fontFamily: "monospace" }}>{m.cpuEst} cores</span></span>
                </div>
              </div>
            ))}

            {MOCK_MUTATIONS.length === 0 && (
              <div style={{ fontSize: 12, color: COLORS.textDim, textAlign: "center", padding: 20 }}>
                No active mutations
              </div>
            )}

            {/* Replication detail in the remaining space */}
            <div style={{ marginTop: 16 }}>
              <SectionTitle icon="🔄">Replication</SectionTitle>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8, fontSize: 11 }}>
                <div style={{ display: "flex", justifyContent: "space-between", color: COLORS.textMuted }}>
                  <span>Tables synced</span>
                  <span style={{ fontFamily: "monospace", color: COLORS.text }}>{MOCK_REPLICATION.healthyTables}/{MOCK_REPLICATION.totalTables}</span>
                </div>
                <div style={{ display: "flex", justifyContent: "space-between", color: COLORS.textMuted }}>
                  <span>Max delay</span>
                  <span style={{ fontFamily: "monospace", color: MOCK_REPLICATION.maxDelay > 30 ? COLORS.warn : COLORS.text }}>{MOCK_REPLICATION.maxDelay}s</span>
                </div>
                <div style={{ display: "flex", justifyContent: "space-between", color: COLORS.textMuted }}>
                  <span>Queue depth</span>
                  <span style={{ fontFamily: "monospace", color: COLORS.text }}>{MOCK_REPLICATION.queueSize}</span>
                </div>
                <div style={{ display: "flex", justifyContent: "space-between", color: COLORS.textMuted }}>
                  <span>Active fetches</span>
                  <span style={{ fontFamily: "monospace", color: COLORS.text }}>{MOCK_REPLICATION.fetchesActive}</span>
                </div>
                <div style={{ display: "flex", justifyContent: "space-between", color: COLORS.textMuted }}>
                  <span>Readonly replicas</span>
                  <span style={{ fontFamily: "monospace", color: MOCK_REPLICATION.readonlyReplicas > 0 ? COLORS.crit : COLORS.ok, fontWeight: 600 }}>
                    {MOCK_REPLICATION.readonlyReplicas > 0 ? `⚠ ${MOCK_REPLICATION.readonlyReplicas}` : "0 ✓"}
                  </span>
                </div>
              </div>
            </div>
          </Card>
        </div>
      </div>

      {/* ===== FOOTER: Explanation ===== */}
      <div style={{ marginTop: 16, padding: "10px 12px", fontSize: 10, color: COLORS.textDim, lineHeight: 1.6, borderTop: `1px solid ${COLORS.border}` }}>
        <strong style={{ color: COLORS.textMuted }}>How attribution works:</strong> CPU for queries comes from live ProfileEvents in <code style={{ color: COLORS.textMuted }}>system.processes</code> (UserTimeMicroseconds + SystemTimeMicroseconds).
        Merge CPU is estimated from recent <code style={{ color: COLORS.textMuted }}>system.part_log</code> ProfileEvents.
        Memory breakdown comes from <code style={{ color: COLORS.textMuted }}>system.processes</code>, <code style={{ color: COLORS.textMuted }}>system.merges</code>, and <code style={{ color: COLORS.textMuted }}>system.asynchronous_metrics</code>.
        "Other" includes replication, TTL, async inserts, and internal housekeeping. Data refreshes every 5-30 seconds.
      </div>
    </div>
  );
}