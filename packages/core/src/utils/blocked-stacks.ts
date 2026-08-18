/**
 * Blocked-stack classification — layer 3 of explaining parked thread time.
 *
 * Layer 1 (query_log ProfileEvents) can say threads were parked but not on what.
 * Layer 2 (processors_profile_log) names which pipeline stage stalled and on
 * which side. This layer names the terminal cause: the actual call the thread
 * was blocked in.
 *
 * It works because `Real` traces fire on a per-thread wall-clock timer, so they
 * sample threads while blocked, not only while running. A sample whose top frame
 * does not symbolize is in kernel space — the thread is in a syscall — and the
 * first ClickHouse frame beneath it names what it is waiting for.
 *
 * Availability is the catch, and it is why this is the deepest layer rather than
 * the only one: it needs `allow_introspection_functions` (a privilege commonly
 * denied to read-only users and on hosted offerings), symbols in the binary, and
 * a non-zero query_profiler_real_time_period_ns. Sampling is also incomplete —
 * coverage measured at 24-65% of RealTimeMicroseconds at a 10ms period — so
 * these are proportions among sampled blocked stacks, never durations.
 */

export type BlockedCategory =
  | 'pipeline'
  | 'remote_shard'
  | 'client'
  | 'queue'
  | 'storage'
  | 'lock'
  | 'housekeeping'
  | 'other';

export interface BlockedStackRow {
  /** Demangled first ClickHouse frame beneath the kernel boundary. */
  blocked_in: string;
  samples: number;
}

export interface BlockedCategorySummary {
  category: BlockedCategory;
  label: string;
  /** What this means, for a tooltip. */
  hint: string;
  samples: number;
  /** Share of sampled blocked stacks, 0..1. */
  share: number;
  /** Frames that classified here, most sampled first. */
  frames: string[];
}

const CATEGORY_META: Record<BlockedCategory, { label: string; hint: string }> = {
  pipeline: {
    label: 'Pipeline idle',
    hint: 'thread parked with no pipeline work ready — usually more threads than the plan can feed',
  },
  remote_shard: {
    label: 'Remote shard',
    hint: 'waiting for a remote replica to answer. Never appears in Network*Elapsed — remote reads poll with epoll rather than blocking reads',
  },
  client: {
    label: 'Client',
    hint: 'blocked delivering results — the client is not consuming as fast as the query produces',
  },
  queue: {
    label: 'Queue handoff',
    hint: 'waiting on an internal async queue between pipeline stages',
  },
  storage: {
    label: 'Storage',
    hint: 'blocked reading from disk or object storage',
  },
  lock: {
    label: 'Lock',
    hint: 'waiting on a mutex, RW lock or ZooKeeper',
  },
  housekeeping: {
    label: 'Housekeeping',
    hint: 'server background work sampled under this query id — log flushing, cancellation checks. Not query work',
  },
  other: {
    label: 'Other',
    hint: 'blocked in a call with no specific classification',
  },
};

/**
 * Frame -> category. Matched as substrings against the demangled symbol, first
 * match wins, so order matters: more specific patterns come first.
 */
const PATTERNS: [string, BlockedCategory][] = [
  // Remote reads poll rather than block, which is exactly why the network
  // ProfileEvents miss them.
  ['Epoll::getManyReady', 'remote_shard'],
  ['RemoteQueryExecutor', 'remote_shard'],
  ['Connection::receivePacket', 'remote_shard'],
  ['HedgedConnections', 'remote_shard'],

  ['WriteBufferFromPocoSocket', 'client'],
  ['ReadBufferFromPocoSocket', 'client'],
  ['LazyOutputFormat', 'client'],
  ['ParallelFormattingOutputFormat', 'client'],
  // The handler thread blocked awaiting the next INSERT block from the client.
  ['TCPHandler::processInsertQuery', 'client'],
  ['TCPHandler::send', 'client'],
  ['TCPHandler::receive', 'client'],

  ['ExecutionThreadContext::wait', 'pipeline'],
  ['PipelineExecutor', 'pipeline'],
  ['ExecutorTasks', 'pipeline'],

  ['ConcurrentBoundedQueue', 'queue'],
  ['ThreadPoolImpl', 'queue'],

  ['ZooKeeper', 'lock'],
  ['Coordination::', 'lock'],
  ['RWLock', 'lock'],
  ['std::__1::mutex', 'lock'],
  ['unique_lock', 'lock'],

  ['ReadBufferFromFile', 'storage'],
  ['WriteBufferFromFile', 'storage'],
  ['AsynchronousReadBuffer', 'storage'],
  ['ThreadPoolReader', 'storage'],
  ['MergeTreeReader', 'storage'],
  ['ReadBufferFromS3', 'storage'],
  ['DiskObjectStorage', 'storage'],

  // Server housekeeping that happens to be sampled under a query's id. Grouped
  // so it does not masquerade as query work.
  ['AsyncLogMessageQueue', 'housekeeping'],
  ['CancellationChecker', 'housekeeping'],
  ['SystemLog', 'housekeeping'],
];

/** Classify one demangled frame. */
export function classifyBlockedFrame(frame: string): BlockedCategory {
  if (!frame) return 'other';
  for (const [needle, category] of PATTERNS) {
    if (frame.includes(needle)) return category;
  }
  return 'other';
}

const num = (value: unknown): number => {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : 0;
};

export function mapBlockedStackRow(row: Record<string, unknown>): BlockedStackRow {
  return {
    blocked_in: String(row.blocked_in ?? ''),
    samples: num(row.samples),
  };
}

/**
 * Group blocked frames into categories, widest first.
 *
 * Shares are of the sampled blocked stacks only — they explain the composition
 * of parked time, not its size.
 */
export function summarizeBlockedStacks(rows: BlockedStackRow[]): BlockedCategorySummary[] {
  const byCategory = new Map<BlockedCategory, { samples: number; frames: BlockedStackRow[] }>();

  for (const row of rows) {
    if (row.samples <= 0) continue;
    const category = classifyBlockedFrame(row.blocked_in);
    const entry = byCategory.get(category) ?? { samples: 0, frames: [] };
    entry.samples += row.samples;
    if (row.blocked_in) entry.frames.push(row);
    byCategory.set(category, entry);
  }

  const total = [...byCategory.values()].reduce((sum, e) => sum + e.samples, 0);
  if (total <= 0) return [];

  return [...byCategory.entries()]
    .map(([category, entry]) => ({
      category,
      label: CATEGORY_META[category].label,
      hint: CATEGORY_META[category].hint,
      samples: entry.samples,
      share: entry.samples / total,
      frames: entry.frames.sort((a, b) => b.samples - a.samples).map(f => f.blocked_in),
    }))
    .sort((a, b) => b.samples - a.samples);
}

/**
 * Minimum samples before shares mean anything.
 *
 * Real traces fire every query_profiler_real_time_period_ns (10ms by default),
 * so short queries produce few or none: measured on a live cluster, only 49% of
 * sub-100ms queries had any sample at all, median zero. Above 100ms coverage is
 * effectively total (99-100%, median 23-209 samples). Below this floor the
 * categories are noise and should be reported as "too few samples" rather than
 * as percentages.
 */
export const MIN_BLOCKED_SAMPLES = 20;

/** Total samples behind a summary, for deciding whether to trust the shares. */
export function totalBlockedSamples(summaries: BlockedCategorySummary[]): number {
  return summaries.reduce((sum, s) => sum + s.samples, 0);
}

/** One-line summary, e.g. "mostly waiting for a remote shard (24%)". */
export function describeBlockedStacks(summaries: BlockedCategorySummary[]): string {
  const top = summaries[0];
  if (!top) return '';
  return `mostly ${top.label.toLowerCase()} (${Math.round(top.share * 100)}%)`;
}
