import React, { useMemo } from "react";
import type {
  DistributedTopology,
  ObjectStorageProfileSummary,
  QueryDetail as QueryDetailType,
  QuerySeries,
  SimilarQuery,
  SubQueryInfo,
} from "@tracehouse/core";
import { queryRowRoleNoun } from "@tracehouse/core";
import { formatBytes } from "../../../../stores/databaseStore";
import {
  formatDurationMs,
  formatMicroseconds,
  formatNumberCompact,
} from "../../../../utils/formatters";
import {
  querySqlLineCount,
  querySqlText,
} from "../../../../utils/querySqlText";
import { SqlHighlight } from "../../../common/SqlHighlight";
import { percentile } from "../shared/chartConstants";

type OverviewTargetTab =
  | "sql"
  | "details"
  | "analytics"
  | "object-storage"
  | "distributed"
  | "logs"
  | "history"
  | "pipeline"
  | "xray"
  | "threads"
  | "flamegraph"
  | "spans";

interface OverviewTabProps {
  q: QuerySeries;
  queryDetail: QueryDetailType | null;
  isSelectQuery: boolean;
  subQueries: SubQueryInfo[];
  distributedTopology: DistributedTopology | null;
  isLoadingSubQueries: boolean;
  similarQueries: SimilarQuery[];
  isLoadingSimilarQueries: boolean;
  objectStorageSummary: ObjectStorageProfileSummary;
  /** Disable all mini charts and illustrations without changing the guidance cards. */
  showMiniVisuals?: boolean;
  showSpansCard?: boolean;
  showLogsCard: boolean;
  showHistoryCard: boolean;
  showXRayCard: boolean;
  showThreadsCard: boolean;
  showFlamegraphCard: boolean;
  onOpenTab: (tab: OverviewTargetTab) => void;
  onNavigateToQuery: (queryId: string) => void;
}

const fmtMs = formatDurationMs;
const fmtUs = formatMicroseconds;

export type ClickHouseExceptionType =
  | "ExceptionBeforeStart"
  | "ExceptionWhileProcessing";

export function clickHouseExceptionType(
  type: string | null | undefined,
): ClickHouseExceptionType | null {
  if (type === "ExceptionBeforeStart" || type === "ExceptionWhileProcessing") {
    return type;
  }
  return null;
}

export function clickHouseExceptionPhase(
  type: ClickHouseExceptionType | null,
): string | null {
  if (type === "ExceptionBeforeStart") return "before execution";
  if (type === "ExceptionWhileProcessing") return "during execution";
  return null;
}

function statusInfo(
  q: QuerySeries,
  detailType?: string,
  detailExceptionCode?: number,
): {
  label: string;
  color: string;
  bg: string;
  code?: number;
  exceptionType: ClickHouseExceptionType | null;
  exceptionPhase: string | null;
} {
  const exceptionType = clickHouseExceptionType(detailType ?? q.status);
  const isFailed =
    exceptionType !== null ||
    (q.exception_code !== undefined && q.exception_code !== 0) ||
    Boolean(q.exception);
  if (q.is_running) {
    return {
      label: "Running",
      color: "#58a6ff",
      bg: "rgba(88, 166, 255, 0.12)",
      exceptionType: null,
      exceptionPhase: null,
    };
  }
  if (isFailed) {
    return {
      label: "Failed",
      color: "var(--color-error)",
      bg: "rgba(var(--color-error-rgb), 0.1)",
      code: detailExceptionCode ?? q.exception_code,
      exceptionType,
      exceptionPhase: clickHouseExceptionPhase(exceptionType),
    };
  }
  return {
    label: "Success",
    color: "var(--color-success)",
    bg: "rgba(var(--color-success-rgb), 0.1)",
    exceptionType: null,
    exceptionPhase: null,
  };
}

function shortId(id: string): string {
  return id.length > 12 ? id.slice(0, 8) : id;
}

function ratioLabel(numerator: number, denominator: number): string {
  if (denominator <= 0) return numerator > 0 ? "no result rows" : "-";
  const ratio = numerator / denominator;
  if (ratio >= 100) return `${Math.round(ratio).toLocaleString()}:1`;
  if (ratio >= 10) return `${ratio.toFixed(1)}:1`;
  return `${ratio.toFixed(2)}:1`;
}

import "./OverviewTab.css";

export const OverviewTab: React.FC<OverviewTabProps> = ({
  q,
  queryDetail,
  isSelectQuery,
  subQueries,
  distributedTopology,
  isLoadingSubQueries,
  similarQueries,
  isLoadingSimilarQueries,
  objectStorageSummary,
  showLogsCard,
  showHistoryCard,
  showXRayCard,
  showThreadsCard,
  showFlamegraphCard,
  showSpansCard = false,
  showMiniVisuals = false,
  onOpenTab,
  onNavigateToQuery,
}) => {
  const status = statusInfo(q, queryDetail?.type, queryDetail?.exception_code);
  const sql = querySqlText(q, queryDetail, "formatted", "");
  const readRows = Number(queryDetail?.read_rows ?? 0);
  const resultRows = Number(queryDetail?.result_rows ?? 0);
  const nodeCount =
    new Set(
      (distributedTopology?.nodes ?? subQueries)
        .map((n) => n.hostname)
        .filter(Boolean),
    ).size || 1;
  const childCount =
    subQueries.length ||
    distributedTopology?.nodes.filter(
      (n) => n.role !== "coordinator" && n.role !== "insert_client",
    ).length ||
    0;
  const parent =
    queryDetail?.is_initial_query === 0 ? queryDetail.initial_query_id : "";
  const history = useMemo(() => {
    const values = similarQueries
      .map((n) => Number(n.query_duration_ms))
      .filter((n) => Number.isFinite(n) && n >= 0);
    return values.length
      ? {
          values,
          median: percentile(
            [...values].sort((a, b) => a - b),
            50,
          ),
        }
      : null;
  }, [similarQueries]);
  const historyText = history
    ? `Median ${fmtMs(history.median)} · ${history.values.length} runs`
    : isLoadingSimilarQueries
      ? "Loading similar runs…"
      : "No similar runs";
  const io =
    Number(queryDetail?.read_bytes ?? q.disk_read ?? 0) +
    Number(q.disk_write ?? 0) +
    Number(q.net_recv ?? 0) +
    Number(q.net_send ?? 0);
  const destinations: Destination[] = [
    {
      tab: "sql",
      title: "SQL",
      question: "Which SQL statement was executed?",
      description: "Query text, tables and columns",
      signal: `${queryDetail?.tables?.length ?? 0} tables · ${queryDetail?.columns?.length ?? 0} columns`,
    },
    {
      tab: "details",
      title: "Internals",
      question: "Where was the time spent?",
      description: "CPU, I/O and execution waits",
      signal: `${fmtMs(q.duration_ms)} · ${formatBytes(q.peak_memory)}`,
    },
    {
      tab: "analytics",
      title: "Analysis",
      question: "How much data did it read?",
      description: "Rows scanned, filtering and results",
      signal: queryDetail
        ? `${formatNumberCompact(readRows)} rows → ${formatNumberCompact(resultRows)}`
        : "Loading query details…",
    },
    ...(childCount > 0 || isLoadingSubQueries
      ? [
          {
            tab: "distributed" as const,
            title: "Distributed",
            question: "How was work shared?",
            description: "Child queries and execution across nodes",
            signal: isLoadingSubQueries
              ? "Loading child queries…"
              : `${nodeCount} nodes · ${childCount} children`,
          },
        ]
      : []),
    ...(showHistoryCard
      ? [
          {
            tab: "history" as const,
            title: "History",
            question: "Is this run typical?",
            description: "Compare duration with similar executions",
            signal: historyText,
          },
        ]
      : []),
    ...(showLogsCard
      ? [
          {
            tab: "logs" as const,
            title: "Logs",
            question: "What did the server report?",
            description: "Query-related server messages",
            signal: "Server logs",
          },
        ]
      : []),
    ...(isSelectQuery
      ? [
          {
            tab: "pipeline" as const,
            title: "Pipeline",
            question: "Where did processors wait?",
            description: "Processor plan, throughput and bottlenecks",
            signal: "DAG · waits · throughput",
          },
        ]
      : []),
    ...(showXRayCard
      ? [
          {
            tab: "xray" as const,
            title: "X-Ray",
            question: "What happened over time?",
            description: "CPU and memory during execution",
            signal: "Process timeline · experimental",
          },
        ]
      : []),
    ...(showThreadsCard
      ? [
          {
            tab: "threads" as const,
            title: "Threads",
            question: "Which threads did the work?",
            description: "Per-thread CPU and memory usage",
            signal: queryDetail?.thread_ids?.length
              ? `${queryDetail.thread_ids.length} threads`
              : "Thread activity",
          },
        ]
      : []),
    ...(showFlamegraphCard
      ? [
          {
            tab: "flamegraph" as const,
            title: "Flamegraph",
            question: "Which functions used CPU?",
            description: "Sampled call stacks and hotspots",
            signal: "CPU profile",
          },
        ]
      : []),
    ...(showSpansCard
      ? [
          {
            tab: "spans" as const,
            title: "Spans",
            question: "How did operations connect?",
            description: "Trace spans and their timing",
            signal: "Trace spans",
          },
        ]
      : []),
    ...(objectStorageSummary.hasObjectStorageIO
      ? [
          {
            tab: "object-storage" as const,
            title: "Object Storage",
            question: "What did remote storage cost?",
            description: "Requests, transferred bytes and I/O time",
            signal: `${formatBytes(objectStorageSummary.bytesRead)} read · ${formatBytes(objectStorageSummary.bytesWritten)} written`,
          },
        ]
      : []),
  ];
  return (
    <div className="query-overview">
      <div className="overview-identity">
        <span
          className="overview-status"
          style={{ color: status.color, background: status.bg }}
        >
          {status.exceptionType ? "error" : status.label}
        </span>
        <Fact label="id" value={shortId(q.query_id)} title={q.query_id} />
        {parent && (
          <Fact
            label="parent"
            value={shortId(parent)}
            title={parent}
            onClick={() => onNavigateToQuery(parent)}
          />
        )}
        <Fact
          label="kind"
          value={(
            queryDetail?.query_kind ||
            q.query_kind ||
            "Query"
          ).toLowerCase()}
        />
        {queryDetail && (
          <Fact
            label="role"
            value={queryRowRoleNoun(queryDetail.is_initial_query !== 0)}
          />
        )}
        <Fact label="user" value={q.user || "-"} />
        <Fact
          label="host"
          value={queryDetail?.hostname || q.hostname || "unknown host"}
        />
      </div>
      {sql && (
        <section className="overview-sql">
          <div className="overview-sql-heading">
            <strong>SQL preview</strong>
            <button onClick={() => onOpenTab("sql")}>Open SQL ↗</button>
          </div>
          <SqlHighlight
            maxHeight={126}
            style={{
              height: Math.min(
                126,
                Math.max(48, querySqlLineCount(sql) * 18 + 16),
              ),
              overflow: "hidden",
              width: "100%",
              fontSize: 12,
              background: "var(--bg-tertiary)",
              borderRadius: 6,
            }}
          >
            {sql}
          </SqlHighlight>
        </section>
      )}
      {(queryDetail?.exception || q.exception) && (
        <section className="overview-error">
          <div>
            {status.exceptionType ?? "ClickHouse exception"}{" "}
            {status.exceptionPhase && <span>{status.exceptionPhase}</span>}{" "}
            {Boolean(status.code) && <span>Code {status.code}</span>}
          </div>
          <pre>{queryDetail?.exception || q.exception}</pre>
        </section>
      )}
      <section className="overview-summary" aria-label="Query summary">
        <Metric
          label="Duration"
          value={fmtMs(q.duration_ms)}
          detail={historyText}
        />
        <Metric
          label="Resources"
          value={formatBytes(q.peak_memory)}
          detail={`CPU ${fmtUs(q.cpu_us)} · I/O ${formatBytes(io)}`}
        />
        <Metric
          label="Rows read → returned"
          value={
            queryDetail
              ? `${formatNumberCompact(readRows)} → ${formatNumberCompact(resultRows)}`
              : "—"
          }
          detail={
            queryDetail
              ? `Scan ratio ${ratioLabel(readRows, resultRows)}`
              : "Loading query details…"
          }
        />
        <Metric
          label="Distribution"
          value={`${nodeCount} ${nodeCount === 1 ? "node" : "nodes"}`}
          detail={
            isLoadingSubQueries
              ? "Loading child queries…"
              : `${childCount} child queries${queryDetail?.thread_ids?.length ? ` · ${queryDetail.thread_ids.length} threads` : ""}`
          }
        />
      </section>
      <section>
        <div className="overview-explore-heading">
          <h3>Explore this query</h3>
        </div>
        <div className="overview-destinations">
          {destinations.map((d) => (
            <button
              className="overview-destination"
              key={d.tab}
              onClick={() => onOpenTab(d.tab)}
            >
              <div className="overview-destination-title">
                <strong>{d.title}</strong>
                <span aria-hidden="true">↗</span>
              </div>
              <h4>{d.question}</h4>
              <p>{d.description}</p>
              <div className="overview-signal">
                <small>{d.signal}</small>
                {showMiniVisuals && (
                  <MiniVisual tab={d.tab} />
                )}
              </div>
            </button>
          ))}
        </div>
      </section>
    </div>
  );
};
interface Destination {
  tab: OverviewTargetTab;
  title: string;
  question: string;
  description: string;
  signal: string;
}
function Fact({
  label,
  value,
  title,
  onClick,
}: {
  label: string;
  value: string;
  title?: string;
  onClick?: () => void;
}) {
  return (
    <div className="overview-fact">
      <span>{label}</span>
      {onClick ? (
        <button onClick={onClick} title={title}>
          {value}
        </button>
      ) : (
        <strong title={title ?? value}>{value}</strong>
      )}
    </div>
  );
}
function Metric({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail: string;
}) {
  return (
    <div>
      <small>{label}</small>
      <strong>{value}</strong>
      <span>{detail}</span>
    </div>
  );
}
const illustrations: Record<OverviewTargetTab, React.ReactNode> = {
  details: <>{[26,17,14,19].map((width,i) => <rect key={i} x={[2,30,49,65][i]} y="7" width={width === 19 ? 13 : width} height="6" rx="2" />)}</>,
  analytics: <><rect x="2" y="4" width="76" height="4" rx="2"/><rect x="2" y="12" width="12" height="4" rx="2"/></>,
  distributed: <>{[76,51,42,34].map((width,i)=><rect key={i} x="2" y={i*5+1} width={width} height="3" rx="1"/>)}</>,
  history: <path d="M2 12L18 8L33 10L49 4L64 7L78 5"/>,
  sql: <path d="M3 4h17m5 0h30M3 10h9m5 0h47M3 16h23m5 0h17" />,
  logs: <path d="M3 4h3m6 0h61M3 10h3m6 0h44M3 16h3m6 0h54" />,
  pipeline: (
    <>
      <path d="M13 10h15m12 0h13M28 10V3h25M28 10v7h25" />
      {[
        [3, 7, 10],
        [30, 7, 10],
        [54, 0, 12],
        [54, 7, 12],
        [54, 14, 12],
      ].map(([x, y, w]) => (
        <rect key={`${x}-${y}`} x={x} y={y} width={w} height="6" rx="2" />
      ))}
    </>
  ),
  xray: (
    <>
      <path d="M2 18h76M3 14l9-2 9 1 9-7 9 4 9-6 9 5 9-1 11 4" />
      <path d="M3 16l10-1 10-1 10 1 10-3 10 1 10-2 14 1" />
    </>
  ),
  threads: (
    <path d="M3 3h9m6 0h21m6 0h12M3 8h23m6 0h14m6 0h24M3 13h15m6 0h35M3 18h31m6 0h12m6 0h15" />
  ),
  flamegraph: (
    <>
      {[
        [2, 15, 76],
        [2, 10, 44],
        [48, 10, 30],
        [2, 5, 19],
        [23, 5, 23],
        [49, 5, 15],
        [24, 0, 14],
      ].map(([x, y, w]) => (
        <rect key={`${x}-${y}`} x={x} y={y} width={w} height="4" rx="1" />
      ))}
    </>
  ),
  spans: <path d="M3 3h72M10 8h35M18 13h19M49 8h21M54 13h12M23 18h11" />,
  "object-storage": (
    <>
      <path d="M8 10h20m24 0h20M28 10l12-7 12 7-12 7Z" />
      <path d="M8 5v10m64-10v10" />
    </>
  ),
};
function MiniVisual({tab}: {tab: OverviewTargetTab}) {
  const visual = illustrations[tab];
  const explanation = "Static illustration of what this tab offers; not measured query data.";
  return (
    <span
      className={`overview-mini overview-mini-${tab}`}
      tabIndex={0}
      role="img"
      aria-label={explanation}
      onClick={(e) => e.stopPropagation()}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") e.stopPropagation();
      }}
    >
      <svg viewBox="0 0 80 20" aria-hidden="true">
        {visual}
      </svg>
      <span className="overview-mini-tooltip" aria-hidden="true">
        {explanation}
      </span>
    </span>
  );
}
