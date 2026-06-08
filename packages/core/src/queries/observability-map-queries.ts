/**
 * SQL probes used to enrich the frontend's static observability map with
 * server-specific system table metadata.
 */

export const OBSERVABILITY_SYSTEM_TABLES = `
SELECT
    name,
    sorting_key,
    primary_key
FROM {{cluster_aware:system.tables}}
WHERE database = 'system'
GROUP BY name, sorting_key, primary_key
ORDER BY name
`;

export const OBSERVABILITY_COLUMN_COMMENTS = `
SELECT
    table,
    name,
    comment
FROM {{cluster_aware:system.columns}}
WHERE database = 'system'
    AND length(comment) > 0
GROUP BY table, name, comment
ORDER BY table, name
`;
