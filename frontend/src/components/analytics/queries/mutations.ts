/**
 * Mutation monitoring queries — active mutations, stuck/failed mutations, progress.
 *
 * Sources:
 * - ClickHouse docs: https://clickhouse.com/docs/operations/system-tables/mutations
 */

const queries: string[] = [
  `-- @meta: title='Active Mutations' group='Mutations' description='Currently running mutations — command, parts remaining, and failure info'
-- @rag: column=parts_to_do green<5 amber<50
-- Source: https://clickhouse.com/docs/operations/system-tables/mutations
SELECT
    database,
    table,
    mutation_id,
    command,
    create_time,
    parts_to_do,
    is_done,
    latest_failed_part,
    latest_fail_time,
    latest_fail_reason
FROM {{cluster_aware:system.mutations}}
WHERE NOT is_done
ORDER BY create_time ASC`,

  `-- @meta: title='Failed / Stuck Mutations' group='Mutations' description='Mutations with failures or that were killed — needs manual investigation'
-- Source: https://clickhouse.com/docs/operations/system-tables/mutations
SELECT
    database,
    table,
    mutation_id,
    command,
    create_time,
    parts_to_do,
    latest_failed_part,
    latest_fail_time,
    latest_fail_reason
FROM {{cluster_aware:system.mutations}}
WHERE latest_fail_reason != ''
ORDER BY latest_fail_time DESC
LIMIT 50`,

  `-- @meta: title='Recent Completed Mutations' group='Mutations' description='Recently finished mutations — useful to verify ALTERs landed'
-- Source: https://clickhouse.com/docs/operations/system-tables/mutations
SELECT
    database,
    table,
    mutation_id,
    command,
    create_time,
    parts_to_do,
    is_done
FROM {{cluster_aware:system.mutations}}
WHERE is_done
ORDER BY create_time DESC
LIMIT 30`,
];

export default queries;
