# Time Travel timezone reproduction

Use the normal Docker environment with the reported server timezone:

```sh
CLICKHOUSE_TIMEZONE=Europe/Copenhagen just docker-start-full-bg
just data-tools-tui
just test-timezone
```

- ClickHouse HTTP: http://localhost:8123
- ClickHouse native: localhost:9000 (the usual TUI defaults)
- Grafana Time Travel: http://localhost:3001/a/dmkskd-tracehouse-app/timetravel
- The existing Docker start variants all accept `CLICKHOUSE_TIMEZONE` through the
  environment. Omit it for UTC. Set it on subsequent start commands too, or export
  it in the shell for the duration of the investigation.
- This sets the actual server `<timezone>`, not just a query session setting.
- Existing volumes and data are preserved. No separate stack is created.
- The reporter's ClickHouse version and other server settings are unknown;
  `CLICKHOUSE_IMAGE` can override the normal development image.

Use the TUI to generate workload as usual. A fresh server has no six-hour history;
the deterministic integration samples cover full six-hour windows. In Grafana,
select Copenhagen as the display preference and use a Copenhagen browser to match
the reporter's local display (Time Travel inputs still use the browser timezone).

Compare the same server version and tests in UTC:

```sh
CLICKHOUSE_TIMEZONE=UTC just docker-start-full-bg
just test-timezone
CLICKHOUSE_TIMEZONE=Europe/Copenhagen just docker-start-full-bg
```

Other IANA timezones work too, for example `America/New_York`. Stop as usual:

```sh
just docker-stop
```

`just test-timezone` uses localhost:8123 by default; `CH_TEST_URL` can override it.

The integration suite can also start its own Copenhagen testcontainer:

```sh
npm run test:integration --workspace packages/core -- src/__tests__/integration/timeline-timezone.integration.test.ts
```

## Results before the fix (2026-11-07)

ClickHouse 26.9.7.9, unchanged production query templates and service mapping:

| Server timezone | Result |
| --- | --- |
| UTC | All 20 regression tests pass |
| Europe/Copenhagen | All 20 regression tests fail on shifted instants or lost points |

Four tests read **real system log rows** through `TimelineService` and the frontend's
`mergeTimelinePoints` range filter. All four metrics returned data but retained zero
visible points in Copenhagen; the UTC control retained all returned points.

Sixteen deterministic tests cover memory, CPU, network, and disk in winter and
summer with five-minute and six-hour windows. Only the SQL log source is replaced
with generated datetime samples, using absolute epochs and the server timezone.
The production projections, UTC bounds, aggregation, service mapping, and frontend
range filter are used unchanged.

| Window | UTC visible / returned | Copenhagen winter | Copenhagen summer |
| --- | --- | --- | --- |
| Five minutes | 5 / 5 | 0 / 5 | 0 / 5 |
| Six hours | 360 / 360 | 301 / 360 | 241 / 360 |

The first winter sample is actually `11:55 UTC`, but SQL returns the string
`12:55` and the frontend interprets it as `12:55 UTC`. The summer shift is two
hours. These initial tests asserted the correct behavior and were red before the fix.

## Results after the fix (2026-11-08)

The expanded suite has 66 passing tests against both Copenhagen and UTC servers.
It covers main/per-host/navigator metrics, historical operation clamping,
microsecond operation identities, detail lookup, running projections, explicit
UTC/Copenhagen/New York column timezones, winter/summer, DST transitions and
midnight crossings. Actual HTTP query output is also passed through the Grafana
adapter with constructed string/time frames to check consistent instants.

The full core suite (972 tests) and frontend suite (1,117 tests) pass. Core,
shared UI, and Grafana plugin builds pass, including the runtime isolation check.
User supplied UI screenshots show the Copenhagen server working in Berlin and
London browser timezones, followed by a verified UTC server continuing to show
data. An automated browser matrix and the actual Grafana datasource transport
are not covered by the new integration suite. See the linked issue for details.
AM/PM formatting and dropdown styling are outside this fix.
