# Merge Tracker

Real-time monitoring of ClickHouse merge operations with dependency diagrams, timelines, and throughput analysis.

## Active Merges

The active merge list shows all currently running merges with:
- Source and result part names
- Progress percentage and elapsed time
- Rows and bytes processed
- Memory usage
- Table and partition info

## Merge Timeline

A visual timeline showing merge operations over time. Each merge is rendered as a bar spanning its start-to-end duration. This helps identify:
- Merge storms (many concurrent merges)
- Long-running merges blocking others
- Patterns in merge scheduling

## Dependency Diagram

The merge dependency diagram shows relationships between merges:
- Which parts are inputs to which merges
- Merge chains (output of one merge becomes input to another)
- Blocked merges waiting for dependencies

## Filter Bar

Filter merges by:
- Table name
- Partition
- Time range
- Merge state (running, completed, failed)

## Merge History

Historical merge data from `system.part_log` with:
- Duration and throughput statistics
- CPU and I/O consumption per merge (from ProfileEvents)
- Merge type (regular, mutation, TTL)
- Size amplification analysis
