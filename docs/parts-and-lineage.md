# Parts & Merge Lineage

## Parts View - How It's Rendered

### Data Sources

| Data | Source Table | Query |
|------|--------------|-------|
| Active parts | `system.parts` | `WHERE active = 1` |
| Running merges | `system.merges` | Real-time snapshot |

### Rendering Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                     Parts 3D Visualization                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. FETCH ACTIVE PARTS                                          │
│     GET /api/databases/{db}/{table}/parts                       │
│     → system.parts WHERE active = 1                             │
│     Returns: name, rows, bytes_on_disk, level, partition_id     │
│                                                                  │
│  2. FETCH RUNNING MERGES                                        │
│     GET /api/merges (polled every 2s)                           │
│     → system.merges                                             │
│     Returns: source_part_names, result_part_name, progress      │
│                                                                  │
│  3. COMBINE & RENDER                                            │
│     - Parts placed in "swim lanes" by merge level (L0→L7)       │
│     - Parts in a merge get highlighted with same color          │
│     - Ghost box shows merge result with progress %              │
│     - Flow lines connect source parts → result                  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Part Positioning (Swim Lanes)

```
     L0          L1          L2          L3          L4    ...
   (inserts)  (1 merge)  (2 merges)  (3 merges)
     
   ┌───┐      ┌───┐      ┌───┐      ┌─────┐
   │   │      │   │      │   │      │     │
   └───┘      └───┘      └───┘      └─────┘
   ┌───┐      ┌───┐      ┌───┐
   │   │      │   │      │   │
   └───┘      └───┘      └───┘
   ┌───┐      ┌───┐
   │   │      │   │
   └───┘      └───┘
     ↑          ↑          ↑
   Small      Medium     Larger
   parts      parts      parts
```

- X-axis: Merge level (extracted from part name `partition_min_max_LEVEL`)
- Z-axis: Position within level (sorted by block range)
- Box size: Proportional to `bytes_on_disk`

### Active Merge Visualization

When `system.merges` returns running merges:

```
Source Parts (highlighted)          Result (ghost box)
┌───┐ ┌───┐ ┌───┐                  ┌─────────────┐
│ A │ │ B │ │ C │  ───────────►   │  MERGING    │
└───┘ └───┘ └───┘                  │   75%       │
  │     │     │                    │  A+B+C → D  │
  └─────┴─────┴────────────────────└─────────────┘
        Flow lines
```

- Source parts: Colored border matching merge
- Result ghost: Semi-transparent box at target level
- Progress: Shown as percentage on ghost box
- Flow lines: Animated lines from sources to result

### Code Flow

```
DatabaseExplorer.tsx
    │
    ├── useDatabaseStore().fetchParts(db, table)
    │       → GET /api/databases/{db}/{table}/parts
    │       → backend queries system.parts
    │
    ├── fetchActiveMerges() [polled every 2s]
    │       → GET /api/merges
    │       → backend queries system.merges
    │
    └── <HierarchyVisualization items={parts} level="parts" />
            │
            ├── calculatePartsLayout() - positions by level
            ├── <MergeLanes /> - swim lane backgrounds
            ├── <MergeFlowLines /> - animated connections
            └── <GlassBox /> - individual part cubes
```

## ClickHouse System Tables Used

### `system.parts`
Active parts currently on disk for MergeTree tables.

```sql
SELECT name, rows, bytes_on_disk, level, partition_id, modification_time
FROM system.parts
WHERE database = 'mydb' AND table = 'mytable' AND active = 1
```

| Column | Description |
|--------|-------------|
| `name` | Part name (e.g., `202602_1_100_3`) |
| `rows` | Number of rows in the part |
| `bytes_on_disk` | Compressed size on disk |
| `level` | Merge level (0 = original insert, higher = more merges) |
| `partition_id` | Partition this part belongs to |
| `active` | 1 = current part, 0 = outdated (pending deletion) |

**Part naming convention:** `{partition}_{minBlock}_{maxBlock}_{level}`
- `202602_1_100_3` = partition 202602, blocks 1-100, merge level 3

### `system.part_log`
Historical log of part events (inserts, merges, deletions). Has TTL - older events get purged.

```sql
SELECT event_time, event_type, part_name, merged_from, size_in_bytes, read_bytes, duration_ms
FROM system.part_log
WHERE database = 'mydb' AND table = 'mytable' AND event_type = 'MergeParts'
```

| Column | Description |
|--------|-------------|
| `event_type` | `NewPart` (insert), `MergeParts` (merge), `RemovePart` (deletion) |
| `part_name` | Resulting part name |
| `merged_from` | Array of source part names (for merges) |
| `size_in_bytes` | Size of resulting part |
| `read_bytes` | Total bytes read (sum of source part sizes) |
| `duration_ms` | How long the operation took |
| `peak_memory_usage` | Memory used during merge |

### `system.merges`
Currently running merge operations (real-time, not historical).

```sql
SELECT result_part_name, source_part_names, progress, elapsed, total_size_bytes_compressed
FROM system.merges
```

## Lineage Building Logic

### Goal
Given a part like `202602_1_9590_7` (L7), trace back through all merges to find the original L0 parts.

### Algorithm

```
1. Start with target part (e.g., L7 part)
2. For each level (L7 → L6 → L5 → ... → L1):
   a. Batch query part_log for all parts at this level
   b. Get merged_from array for each part
   c. Collect all source parts for next level
3. Query L0 part sizes from NewPart events (they have no merge history)
4. Build tree from cached data (no more DB queries)
```

### Key Insight: `read_bytes` vs `size_in_bytes`

**WARNING:** `read_bytes` is the **uncompressed** bytes read during the merge, NOT the compressed on-disk sizes of source parts.

- `read_bytes` = uncompressed data read during merge (larger value)
- `size_in_bytes` = compressed on-disk size of the resulting part

For calculating space savings, you must sum the `size_in_bytes` of L0 parts (from their `NewPart` events), NOT use `read_bytes`.

### Batch Query Strategy
Instead of querying each part individually (thousands of queries), we batch by level:

```sql
-- One query per level, fetches all merge events at once
SELECT part_name, merged_from, size_in_bytes, read_bytes, duration_ms
FROM system.part_log
WHERE database = 'mydb' AND table = 'mytable'
  AND part_name IN ('part1', 'part2', 'part3', ...)  -- batch
  AND event_type = 'MergeParts'
```

For an L7 part: ~7-8 batch queries total (one per level + L0 sizes).

### Space Savings Calculation

**IMPORTANT:** `read_bytes` in `system.part_log` is the **uncompressed** bytes read during the merge operation, NOT the compressed on-disk sizes of source parts. Using `read_bytes` will give incorrect (inflated) original size values.

**Correct Formula:**
```
Original Size = sum of all L0 part sizes (size_in_bytes from NewPart events in part_log)
Final Size = current part size (bytes_on_disk from system.parts)
Space Saved = (Original - Final) / Original * 100%
```

**Algorithm:**
1. For L0 leaf nodes: use their `size_in_bytes` (compressed on-disk size)
2. For L1+ merges: recursively sum children's `size_in_bytes`
3. Do NOT use `read_bytes` - it represents uncompressed data read during merge

**Example:**
```
6 L0 parts × 24.7 MB each = 148.2 MB (original compressed size)
L1 result part = 149.94 MB
Space Saved = (148.2 - 149.94) / 148.2 = -1.2% (slight expansion)
```

**Common Mistake:**
Using `read_bytes` (e.g., 331.88 MB uncompressed) instead of summing L0 `size_in_bytes` (148.2 MB compressed) will show incorrect savings like "+54.8%" when the actual result is slight expansion.

For individual merge operations:
```
source_total_size = sum of source part size_in_bytes (NOT read_bytes)
space_savings = source_total_size - result_size_in_bytes
```

### Limitations

1. **part_log TTL**: Old merge events get purged, so very old lineage may be incomplete
2. **L0 parts merged away**: Original insert parts no longer exist in system.parts
3. **Large parts**: Parts with 10k+ source parts are limited to 5000 nodes to prevent timeout

## Example Lineage Tree

```
202602_1_9590_7 (L7) - 500 MB
├── 202602_1_5000_6 (L6) - 300 MB
│   ├── 202602_1_2500_5 (L5) - 180 MB
│   │   └── ... (more merges)
│   └── 202602_2501_5000_5 (L5) - 150 MB
│       └── ...
└── 202602_5001_9590_6 (L6) - 280 MB
    └── ...
        └── 202602_9500_9590_0 (L0) - 5 MB  ← Original insert
```

## API Endpoint

```
GET /api/databases/{db}/{table}/parts/{part_name}/lineage?connection_id={id}
```

Response includes:
- `root`: Tree structure with all merge nodes
- `total_merges`: Count of merge operations
- `total_original_parts`: Count of L0 parts
- `original_total_size`: Sum of L0 sizes
- `final_size`: Current part size
- `overall_space_savings_percent`: Total compression achieved
