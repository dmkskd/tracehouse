import { describe, it, expect } from 'vitest';
import {
  classifyBlockedFrame,
  summarizeBlockedStacks,
  describeBlockedStacks,
  mapBlockedStackRow,
} from '../blocked-stacks.js';

// Frames taken verbatim from a live cluster's trace_log.
const REAL_FRAMES = {
  pipelineWait: 'DB::ExecutionThreadContext::wait(std::__1::atomic<bool>&)',
  epoll: 'DB::Epoll::getManyReady(int, epoll_event*, int) const',
  socketSend: 'DB::WriteBufferFromPocoSocket::socketSendBytesImpl(char const*, unsigned long)',
  lazyOutput: 'DB::LazyOutputFormat::consume(DB::Chunk)',
  queuePop: 'bool ConcurrentBoundedQueue<DB::Chunk>::popImpl<true>(DB::Chunk&, std::__1::optional<unsigned long>)',
  zookeeper: 'DB::StorageReplicatedMergeTree::checkPartChecksumsAndAddCommitOps(std::__1::shared_ptr<DB::ZooKeeperWithFaultInjection> const&)',
};

describe('classifyBlockedFrame', { tags: ['observability'] }, () => {
  it('classifies the frames actually seen on a live cluster', () => {
    expect(classifyBlockedFrame(REAL_FRAMES.pipelineWait)).toBe('pipeline');
    expect(classifyBlockedFrame(REAL_FRAMES.epoll)).toBe('remote_shard');
    expect(classifyBlockedFrame(REAL_FRAMES.socketSend)).toBe('client');
    expect(classifyBlockedFrame(REAL_FRAMES.lazyOutput)).toBe('client');
    expect(classifyBlockedFrame(REAL_FRAMES.queuePop)).toBe('queue');
    expect(classifyBlockedFrame(REAL_FRAMES.zookeeper)).toBe('lock');
  });

  it('falls back to other for unknown or empty frames', () => {
    expect(classifyBlockedFrame('DB::SomethingNobodyHasSeen::run()')).toBe('other');
    expect(classifyBlockedFrame('')).toBe('other');
  });

  it('routes storage reads to storage, not queue', () => {
    expect(classifyBlockedFrame('DB::ReadBufferFromFileDescriptor::nextImpl()')).toBe('storage');
    expect(classifyBlockedFrame('DB::ReadBufferFromS3::nextImpl()')).toBe('storage');
  });
});

describe('summarizeBlockedStacks', { tags: ['observability'] }, () => {
  // Distribution measured on query b5e19c26 — a read back-pressured by a slow
  // client, which no single layer names on its own.
  const rows = [
    { blocked_in: REAL_FRAMES.pipelineWait, samples: 1718 },
    { blocked_in: REAL_FRAMES.epoll, samples: 1300 },
    { blocked_in: REAL_FRAMES.socketSend, samples: 849 },
    { blocked_in: REAL_FRAMES.lazyOutput, samples: 837 },
    { blocked_in: REAL_FRAMES.queuePop, samples: 447 },
  ];

  it('groups frames into categories and shares, widest first', () => {
    const summary = summarizeBlockedStacks(rows);

    expect(summary.map(s => s.category)).toEqual([
      'client',       // 849 + 837 = 1686
      'pipeline',     // 1718
      'remote_shard', // 1300
      'queue',        // 447
    ].sort((a, b) => {
      const order: Record<string, number> = { pipeline: 1718, client: 1686, remote_shard: 1300, queue: 447 };
      return order[b] - order[a];
    }));

    const total = summary.reduce((sum, s) => sum + s.share, 0);
    expect(total).toBeCloseTo(1, 6);
  });

  it('merges the two client-side frames into one category', () => {
    const client = summarizeBlockedStacks(rows).find(s => s.category === 'client');
    expect(client?.samples).toBe(849 + 837);
    expect(client?.frames).toHaveLength(2);
    // Most-sampled frame first, so the tooltip leads with the dominant call.
    expect(client?.frames[0]).toBe(REAL_FRAMES.socketSend);
  });

  it('keeps remote shard visible — the wait Network*Elapsed misses entirely', () => {
    const remote = summarizeBlockedStacks(rows).find(s => s.category === 'remote_shard');
    expect(remote?.samples).toBe(1300);
    expect(remote?.share).toBeCloseTo(1300 / 5151, 4);
  });

  it('returns nothing when there is nothing sampled', () => {
    expect(summarizeBlockedStacks([])).toEqual([]);
    expect(summarizeBlockedStacks([{ blocked_in: 'x', samples: 0 }])).toEqual([]);
  });

  it('parses string sample counts from the HTTP interface', () => {
    const mapped = mapBlockedStackRow({ blocked_in: REAL_FRAMES.epoll, samples: '1300' });
    expect(mapped.samples).toBe(1300);
    expect(summarizeBlockedStacks([mapped])[0].category).toBe('remote_shard');
  });

  it('describes the dominant category in one line', () => {
    expect(describeBlockedStacks(summarizeBlockedStacks(rows))).toMatch(/^mostly /);
    expect(describeBlockedStacks([])).toBe('');
  });
});
