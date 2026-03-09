# Database Explorer

Browse your ClickHouse databases, tables, parts, and columns with detailed metadata and visualizations.

## Database Tree

The left panel shows a hierarchical tree of all databases and tables. Click any table to see its detail view.

## Table Detail

For each table, you can inspect:

- **Schema** - Column names, types, compression codecs, and sizes
- **Parts** - Active data parts with size, rows, level, and partition info
- **Part Inspector** - Inspect individual parts with column-level statistics
- **Merge Timeline** - Historical merge activity for the table
- **Lineage** - Visual merge lineage tree showing how parts were created and merged

## Parts Visualization

The 3D parts view renders each data part as a block, sized proportionally to its data volume. Colors indicate:
- Part level (how many merges it has been through)
- Partition membership
- Active vs. inactive state

## Merge Lineage

The lineage visualization traces how parts evolve over time:
1. New parts created by INSERTs
2. Merges combining multiple parts into one
3. Mutations rewriting parts
4. The resulting part hierarchy

This helps diagnose issues like:
- Parts not merging (too many small parts)
- Unbalanced merge trees
- Stuck mutations blocking merges
