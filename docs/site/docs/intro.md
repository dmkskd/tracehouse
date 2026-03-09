# Introduction

TraceHouse is a real-time monitoring and visualization tool for ClickHouse databases. It shows what's happening inside your ClickHouse cluster through resource attribution, visual exploration, and system table analysis.

## What It Does

Instead of just showing "CPU is at 200%", it breaks that down: 120% from queries, 60% from merges, 15% from a stuck mutation on table Y. Same for memory, disk I/O, etc.

It watches three things in a running ClickHouse system:

### Server Resources

CPU cores, memory, disks, network. The physical limits and how they're being used.

### Queries

Clients submitting SELECTs, INSERTs, DDL. Each query consumes resources and competes for shared capacity.

### Background Work

ClickHouse's asynchronous operations:

- **Merges** - combining smaller parts into larger ones
- **Mutations** - ALTER UPDATE/DELETE rewriting parts
- **Replication** - fetching parts, replaying the replication log
- **Materialized Views** - triggered on insert, transforming and routing data
- **TTL Processing** - moving, re-compressing, or deleting expired data

## Key Features

| Feature | Description |
|---------|-------------|
| Cluster Overview | Real-time CPU, memory, disk I/O with resource attribution |
| Database Explorer | Browse databases, tables, parts, columns, and merge lineage |
| Merge Tracker | Live merge monitoring with dependency diagrams and timelines |
| Query Monitor | Running queries with per-query resource breakdown |
| Engine Internals | Thread pools, memory allocators, CPU sampling, PK index analysis |
| Analytics | Custom dashboards, preset queries, and ordering key diagnostics |

## Next Steps

- [Getting Started](./getting-started) - run the app in under a minute
- [Deployment](./guides/deployment) - build and ship for production
- [Architecture](./architecture) - understand the project structure
