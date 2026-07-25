export interface ExecutionAnalysisSummary {
  totalTime?: string;
  totalTimeMs?: number;
  planningTime?: string;
  planningTimeMs?: number;
  executionTime?: string;
  executionTimeMs?: number;
  readRows?: string;
  readBytes?: string;
  rowsPerSecond?: string;
  bytesPerSecond?: string;
  peakMemory?: string;
  output?: string;
}

export interface ExecutionAnalysisIO {
  inputRows: string;
  outputRows: string;
  retainedRows?: string;
  inputBytes: string;
  outputBytes: string;
}

export interface ExecutionAnalysisProcessorTiming {
  count: number;
  min: string;
  median: string;
  max: string;
  sum: string;
}

export interface ExecutionAnalysisTiming {
  label?: string;
  duration: string;
  durationUs?: number;
  share: number;
  parallelism: number;
  maxParallelism: number;
  processors?: ExecutionAnalysisProcessorTiming;
}

export interface ExecutionAnalysisNode {
  id: string;
  name: string;
  description?: string;
  depth: number;
  io?: ExecutionAnalysisIO;
  timings: ExecutionAnalysisTiming[];
  details: string[];
}

export interface ParsedExecutionAnalysis {
  summary: ExecutionAnalysisSummary;
  nodes: ExecutionAnalysisNode[];
}

export interface SelectionRatio {
  selected: number;
  total: number;
}

export interface MergeTreeIndexAnalysis {
  type: string;
  name?: string;
  description?: string;
  condition?: string;
  keys: string[];
  parts?: SelectionRatio;
  granules?: SelectionRatio;
  searchAlgorithm?: string;
}

export interface MergeTreeReadAnalysis {
  readType?: string;
  parts?: number;
  granules?: number;
  ranges?: number;
  outputColumns: string[];
  prewhere?: string;
  indexes: MergeTreeIndexAnalysis[];
}

export interface OperatorDetailField {
  raw: string;
  label?: string;
  value?: string;
}

function durationToMs(value: string): number | undefined {
  const match = value.trim().match(/^([\d.]+)\s*(ns|us|µs|ms|s)$/i);
  if (!match) return undefined;

  const amount = Number(match[1]);
  if (!Number.isFinite(amount)) return undefined;

  switch (match[2].toLowerCase()) {
    case 'ns':
      return amount / 1_000_000;
    case 'us':
    case 'µs':
      return amount / 1_000;
    case 'ms':
      return amount;
    case 's':
      return amount * 1_000;
    default:
      return undefined;
  }
}

function formattedCount(value: string): number | undefined {
  const match = value.trim().replace(/,/g, '').match(
    /^([\d.]+)(?:\s+(thousand|million|billion))?$/i,
  );
  if (!match) return undefined;

  const amount = Number(match[1]);
  if (!Number.isFinite(amount)) return undefined;

  const multiplier = {
    thousand: 1_000,
    million: 1_000_000,
    billion: 1_000_000_000,
  }[match[2]?.toLowerCase() ?? ''] ?? 1;

  return amount * multiplier;
}

function parseSummary(lines: string[]): ExecutionAnalysisSummary {
  const summary: ExecutionAnalysisSummary = {};

  for (const line of lines) {
    const trimmed = line.trim();
    const time = trimmed.match(
      /^Time:\s+(.+?)\s+\(planning (.+?)\s+·\s+execution (.+?)\)$/,
    );
    if (time) {
      summary.totalTime = time[1];
      summary.totalTimeMs = durationToMs(time[1]);
      summary.planningTime = time[2];
      summary.planningTimeMs = durationToMs(time[2]);
      summary.executionTime = time[3];
      summary.executionTimeMs = durationToMs(time[3]);
      continue;
    }

    const read = trimmed.match(/^Read:\s+(.+?) rows,\s+(.+?)\s+\((.+?) rows\/s\.,\s+(.+?)\)$/);
    if (read) {
      summary.readRows = read[1];
      summary.readBytes = read[2];
      summary.rowsPerSecond = read[3];
      summary.bytesPerSecond = read[4];
      continue;
    }

    const peakMemory = trimmed.match(/^Peak memory:\s+(.+)$/);
    if (peakMemory) {
      summary.peakMemory = peakMemory[1];
      continue;
    }

    const output = trimmed.match(/^Output:\s+(.+)$/);
    if (output && !summary.output) summary.output = output[1];
  }

  return summary;
}

function planNodeLine(
  line: string,
  isFirstPlanLine: boolean,
): { depth: number; text: string } | undefined {
  const branch = line.match(/^((?:(?:│ {2}| {3}))*)(?:├──|└──)(\S.*)$/);
  if (branch) {
    return {
      depth: (branch[1].length / 3) + 1,
      text: branch[2].trim(),
    };
  }

  if (isFirstPlanLine && /^[A-Za-z][A-Za-z0-9_]*(?:\s|\(|$)/.test(line.trim())) {
    return { depth: 0, text: line.trim() };
  }

  return undefined;
}

function nodeIdentity(text: string): { name: string; description?: string } {
  const match = text.match(/^([A-Za-z][A-Za-z0-9_]*)(?:\s+(.+))?$/);
  if (!match) return { name: text };

  const description = match[2]?.trim();
  return {
    name: match[1],
    description: description
      ? description.replace(/^\((.*)\)$/, '$1')
      : undefined,
  };
}

function detailText(line: string): string {
  return line
    .replace(/^(?:(?:│ {2}| {3}))*(?:├──|└──)?/, '')
    .trim();
}

function parseIO(text: string): ExecutionAnalysisIO | undefined {
  const match = text.match(
    /^I\/O: rows (.+?) → (.+?)(?: \(([^)]+)\))? · (.+?) → (.+)$/,
  );
  if (!match) return undefined;

  const inputRows = match[1];
  const outputRows = match[2];
  let retainedRows = match[3];

  if (!retainedRows) {
    const input = formattedCount(inputRows);
    const output = formattedCount(outputRows);
    if (input && output !== undefined && output <= input) {
      retainedRows = `${((output / input) * 100).toFixed(1)}%`;
    }
  }

  return {
    inputRows,
    outputRows,
    retainedRows,
    inputBytes: match[4],
    outputBytes: match[5],
  };
}

function parseTiming(text: string): ExecutionAnalysisTiming | undefined {
  const match = text.match(
    /^(?:Stage \((.+)\): )?time (.+?) \(([\d.]+)%\) · parallelism ([\d.]+)\/(\d+)$/,
  );
  if (!match) return undefined;

  return {
    label: match[1],
    duration: match[2],
    durationUs: durationToMs(match[2]) !== undefined
      ? durationToMs(match[2])! * 1_000
      : undefined,
    share: Number(match[3]),
    parallelism: Number(match[4]),
    maxParallelism: Number(match[5]),
  };
}

function parseProcessorTiming(text: string): ExecutionAnalysisProcessorTiming | undefined {
  const match = text.match(
    /^Time per processor \((\d+)\): min (.+?) · median (.+?) · max (.+?) · sum (.+)$/,
  );
  if (!match) return undefined;

  return {
    count: Number(match[1]),
    min: match[2],
    median: match[3],
    max: match[4],
    sum: match[5],
  };
}

/**
 * Best-effort projection of ClickHouse's evolving EXPLAIN ANALYZE text.
 *
 * Parsing failure is intentionally non-fatal: callers always retain the raw
 * output as the authoritative final tab.
 */
export function parseExecutionAnalysis(output: string): ParsedExecutionAnalysis {
  const lines = output.split(/\r?\n/);
  const summary = parseSummary(lines);
  const nodes: ExecutionAnalysisNode[] = [];
  let currentNode: ExecutionAnalysisNode | undefined;
  let planStarted = false;

  for (const line of lines) {
    if (!planStarted) {
      if (line.trim().startsWith('Output:')) planStarted = true;
      continue;
    }

    if (!line.trim()) continue;

    const nodeLine = planNodeLine(line, nodes.length === 0);
    if (nodeLine) {
      const identity = nodeIdentity(nodeLine.text);
      currentNode = {
        id: `execution-node-${nodes.length}`,
        name: identity.name,
        description: identity.description,
        depth: nodeLine.depth,
        timings: [],
        details: [],
      };
      nodes.push(currentNode);
      continue;
    }

    if (!currentNode) continue;
    const text = detailText(line);
    if (!text) continue;

    const io = parseIO(text);
    if (io) {
      currentNode.io = io;
      continue;
    }

    const timing = parseTiming(text);
    if (timing) {
      currentNode.timings.push(timing);
      continue;
    }

    const processorTiming = parseProcessorTiming(text);
    if (processorTiming && currentNode.timings.length > 0) {
      currentNode.timings[currentNode.timings.length - 1].processors = processorTiming;
      continue;
    }

    currentNode.details.push(text);
  }

  return { summary, nodes };
}

/** Present ClickHouse's result-first tree as the source-to-result execution flow. */
export function buildExecutionFlowNodes(
  nodes: ExecutionAnalysisNode[],
): ExecutionAnalysisNode[] {
  const maxDepth = Math.max(0, ...nodes.map(node => node.depth));
  return [...nodes]
    .reverse()
    .map(node => ({
      ...node,
      depth: maxDepth - node.depth,
    }));
}

/** Split generic operator details into displayable label/value fields. */
export function parseOperatorDetailFields(lines: string[]): OperatorDetailField[] {
  return lines.map(raw => {
    const field = raw.match(/^([^:]{1,48}):\s*(.*)$/);
    if (!field?.[2]) return { raw };
    return {
      raw,
      label: field[1],
      value: field[2],
    };
  });
}

function selectionRatio(value: string): SelectionRatio | undefined {
  const match = value.match(/^(\d+)\/(\d+)$/);
  if (!match) return undefined;
  return { selected: Number(match[1]), total: Number(match[2]) };
}

const INDEX_TYPES = new Set([
  'Min-Max',
  'Partition',
  'PrimaryKey',
  'Skip',
]);

/**
 * Project the detail lines emitted by ReadFromMergeTree into scan indicators.
 * Unknown lines remain available in the raw operator details.
 */
export function parseMergeTreeReadDetails(details: string[]): MergeTreeReadAnalysis {
  const analysis: MergeTreeReadAnalysis = {
    outputColumns: [],
    indexes: [],
  };
  let inIndexes = false;
  let currentIndex: MergeTreeIndexAnalysis | undefined;
  let collectingKeys = false;

  for (const line of details) {
    if (line === 'Indexes:') {
      inIndexes = true;
      collectingKeys = false;
      continue;
    }

    if (inIndexes && INDEX_TYPES.has(line)) {
      currentIndex = { type: line, keys: [] };
      analysis.indexes.push(currentIndex);
      collectingKeys = false;
      continue;
    }

    const readType = line.match(/^Read type:\s*(.+)$/);
    if (!inIndexes && readType) {
      analysis.readType = readType[1];
      continue;
    }

    const readSelection = line.match(/^Parts:\s*(\d+)\s*\|\s*Granules:\s*(\d+)$/);
    if (!inIndexes && readSelection) {
      analysis.parts = Number(readSelection[1]);
      analysis.granules = Number(readSelection[2]);
      continue;
    }

    const outputColumns = line.match(/^Output:\s*(.+)$/);
    if (!inIndexes && outputColumns) {
      analysis.outputColumns = outputColumns[1]
        .split(',')
        .map(column => column.trim())
        .filter(Boolean);
      continue;
    }

    const prewhere = line.match(/^Prewhere filter column:\s*(.+)$/);
    if (!inIndexes && prewhere) {
      analysis.prewhere = prewhere[1];
      continue;
    }

    const ranges = line.match(/^Ranges:\s*(\d+)$/);
    if (ranges) {
      analysis.ranges = Number(ranges[1]);
      collectingKeys = false;
      continue;
    }

    if (!currentIndex) continue;

    if (line === 'Keys:') {
      collectingKeys = true;
      continue;
    }

    const condition = line.match(/^Condition:\s*(.+)$/);
    if (condition) {
      currentIndex.condition = condition[1];
      collectingKeys = false;
      continue;
    }

    const parts = line.match(/^Parts:\s*(\d+\/\d+)$/);
    if (parts) {
      currentIndex.parts = selectionRatio(parts[1]);
      collectingKeys = false;
      continue;
    }

    const granules = line.match(/^Granules:\s*(\d+\/\d+)$/);
    if (granules) {
      currentIndex.granules = selectionRatio(granules[1]);
      collectingKeys = false;
      continue;
    }

    const name = line.match(/^Name:\s*(.+)$/);
    if (name) {
      currentIndex.name = name[1];
      collectingKeys = false;
      continue;
    }

    const indexDescription = line.match(/^Description:\s*(.+)$/);
    if (indexDescription) {
      currentIndex.description = indexDescription[1];
      collectingKeys = false;
      continue;
    }

    const searchAlgorithm = line.match(/^Search Algorithm:\s*(.+)$/);
    if (searchAlgorithm) {
      currentIndex.searchAlgorithm = searchAlgorithm[1];
      collectingKeys = false;
      continue;
    }

    if (collectingKeys) currentIndex.keys.push(line);
  }

  return analysis;
}
