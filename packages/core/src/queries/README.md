# Timestamp contract for query authors

Use **UTC for timestamp instants in all new and changed queries**, including SQL
embedded in services and built-in Analytics queries. Convert to the user's local
timezone only when formatting the UI. A ClickHouse server may run in any timezone;
Grafana and standalone transports must represent the same absolute instant.

- Bind absolute range parameters with `utcDateTime()` / `utcDateTime64()` from
  `builder.ts`. Do not interpolate timezone-less datetime literals.
- Project string timestamps explicitly: `toString(event_time, 'UTC')`. Raw
  DateTime fields and plain `toString(event_time)` can be serialized differently
  by the direct client and Grafana datasource. Never depend on Grafana correcting
  a projection for the standalone app.
- Pair UTC projections with the shared absolute timestamp parser or an explicit
  UTC mapper. Do not parse an unmarked server-local string as though it were UTC.
  Do not remove an explicit offset, append a second `Z`, or convert an instant twice.
- Keep columns used by predicates and aggregation typed. Use a separate UTC
  projection alias where alias substitution could change filtering or grouping.
- Parse timestamp **string** lookup parameters with an explicit UTC SQL timezone,
  e.g. `toDateTime64({instant}, 6, 'UTC')`. Preserve exact microsecond identities
  as strings; a JavaScript `Date` truncates them to milliseconds. New persisted
  identity strings must include an explicit timezone. Handle legacy unzoned keys
  deliberately; never guess between ambiguous attempts.
- Use `eventDateBound()` when a UTC instant supplies a lower bound for a local
  `event_date` partition. Keep the exact absolute-time predicate as well. Rolling
  windows that cross midnight must also include the preceding local date.
- Test differing server/source/browser zones and both direct and Grafana paths.
  Equal-looking labels alone do not prove equal epochs or exact identities.

Interactive Analytics SQL requests typed UTC DateTime output at the transport
boundary, including ProxyAdapter's forwarded ClickHouse HTTP settings. Test that
path separately from the direct browser client. The Grafana path also describes native result types because its frame
schema otherwise conflates calendar Date and DateTime. This does not normalize
arbitrary SQL strings: authors must still project timestamp strings in UTC. Exact
microsecond identities must use strings, since Grafana time frames retain only
milliseconds.

Native-result metadata SQL belongs in `result-type-queries.ts`. Keep output
clauses and statement terminators out of the nested query using its SQL-aware
scanner; do not strip them with a regex that can match quoted text or comments.
Display localization must retain the source fractional digits. Optional system
clocks may use epoch zero as absence; apply that policy to those fields only.

Numeric epochs keep their documented units. Calendar `Date` values, intentional
local-calendar analysis and arbitrary user-authored strings are separate concepts:
do not reinterpret them as UTC instants without a declared contract.

Existing migrations and remaining exceptions are tracked in the
[implementation plan](../../../../docs/development/timezone-implementation-plan.md).
