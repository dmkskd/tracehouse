/**
 * Access: failures, who used which table, and privileged principals. User, role and table cells
 * open the object browser (@object_link); a table opens with that row's user in context.
 */
import { APP_SOURCE_LIKE } from '@tracehouse/core';

const queries: string[] = [
  `-- @meta: title='Access Failures by User' group='Access' interval='1 DAY' description='Queries rejected for privileges, read-only mode, setting constraints or quotas, per user and error. Click a user to see what they can reach.'
-- @chart: type=stacked_bar group_by=user value=failures series=error style=2d
-- @object_link: on=user kind=user user=user
SELECT
    user,
    errorCodeToName(exception_code) AS error,
    count() AS failures
FROM {{cluster_aware:system.query_log}}
WHERE type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
  AND exception_code IN (497, 164, 452, 201)
  AND event_date >= toDate({{time_range}})
  AND event_time > {{time_range}}
  AND query NOT LIKE ${APP_SOURCE_LIKE}
GROUP BY user, error
ORDER BY failures DESC
LIMIT 100`,

  `-- @meta: title='Recent Access Failures' group='Access' interval='1 DAY' description='Each query refused for a missing privilege, with the grant ClickHouse said was missing. Click the user, the table (opened for that user) or the query.'
-- @object_link: on=user kind=user user=user
-- @object_link: on=table kind=table database=database table=table user=user
-- @query_link: on=query_id
SELECT
    toString(event_time, 'UTC') AS time,
    user,
    database,
    table,
    if(reason != '', reason, substring(exception, 1, 200)) AS missing,
    query_id
FROM
(
    SELECT
        event_time,
        user,
        exception,
        query_id,
        -- query_log.tables is empty for a query refused before start; the message names the object.
        extractAllGroups(exception, '(?i) on ([a-z0-9_]+)[.]([a-z0-9_]+)') AS objects,
        if(length(objects) > 0, objects[1][1], '') AS database,
        if(length(objects) > 0, objects[1][2], '') AS table,
        extract(exception, 'necessary to have the grant (.*?)[.] [(]ACCESS_DENIED') AS reason
    FROM {{cluster_aware:system.query_log}}
    WHERE type IN ('ExceptionBeforeStart', 'ExceptionWhileProcessing')
      AND exception_code = 497
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND query NOT LIKE ${APP_SOURCE_LIKE}
)
ORDER BY event_time DESC
LIMIT 200`,

  `-- @meta: title='Table Usage by User' group='Access' interval='1 DAY' description='Which user used which table, as recorded in query_log: finished queries, and queries refused for a missing privilege. Queries the server runs without a user (refreshable materialized views) are not listed. Click a table to see the access paths for that user.'
-- @object_link: on=table kind=table database=database table=table user=user
-- @object_link: on=user kind=user user=user
SELECT
    user,
    splitByChar('.', qualified)[1] AS database,
    arrayStringConcat(arraySlice(splitByChar('.', qualified), 2), '.') AS table,
    countIf(type = 'QueryFinish') AS queries,
    countIf(exception_code = 497) AS refused
FROM
(
    SELECT
        user,
        type,
        exception_code,
        -- A query refused before start has no tables; the message names the object.
        if(exception_code = 497 AND empty(tables),
           arrayMap(g -> g[1] || '.' || g[2], arraySlice(extractAllGroups(exception, '(?i) on ([a-z0-9_]+)[.]([a-z0-9_]+)'), 1, 1)),
           tables) AS used
    FROM {{cluster_aware:system.query_log}}
    WHERE type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing')
      AND event_date >= toDate({{time_range}})
      AND event_time > {{time_range}}
      AND user != ''
      AND query NOT LIKE ${APP_SOURCE_LIKE}
)
ARRAY JOIN used AS qualified
WHERE splitByChar('.', qualified)[1] NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA', '_table_function')
GROUP BY user, database, table
HAVING queries > 0 OR refused > 0
ORDER BY user, database, table
LIMIT 500`,

  `-- @meta: title='Privileged Principals' group='Access' interval='1 DAY' description='Users and roles holding ALL, any privilege on every database, or the right to grant one. Click a name to see its access.'
-- @object_link: on=user kind=user user=user
-- @object_link: on=role kind=role role=role
SELECT
    user,
    role,
    scope,
    if(has(granted, 'ALL'), 'ALL', arrayStringConcat(arraySlice(arraySort(granted), 1, 5), ', ')
        || if(length(granted) > 5, concat(' +', toString(length(granted) - 5), ' more'), '')) AS privileges,
    if(with_grant_option, 'yes', '') AS can_grant
FROM
(
    SELECT
        ifNull(user_name, '') AS user,
        ifNull(role_name, '') AS role,
        if(database IS NULL, '*', database) || '.' || if(table IS NULL, '*', table) AS scope,
        groupUniqArray(toString(access_type)) AS granted,
        max(grant_option) AS with_grant_option
    FROM {{cluster_aware:system.grants}}
    WHERE (access_type = 'ALL' OR database IS NULL OR grant_option)
      AND is_partial_revoke = 0
    GROUP BY user, role, scope
)
ORDER BY user, role, scope
LIMIT 500`,
];

export default queries;
