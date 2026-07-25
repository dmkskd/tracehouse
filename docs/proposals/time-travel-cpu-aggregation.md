# Time Travel CPU Aggregation

Status: open note / follow-up.

## Current model

Time Travel "All" mode currently represents average per-host CPU saturation, not total cluster CPU capacity.

- CPU samples come from `system.asynchronous_metric_log` through the cluster-aware query path.
- For each timestamp and host, CPU is computed from `CGroupUserTime + CGroupSystemTime` when available, otherwise `OSUserTime + OSSystemTime`.
- The server CPU line then averages those per-host samples across the selected hosts.
- The denominator is the effective per-host core count, using cgroup limits when available and physical core count as fallback.
- Therefore `100%` means "the average selected host is fully saturated", not "the entire selected cluster is fully saturated".

For example, on a 4-node cluster with 4 effective cores per node, the physical cluster capacity is 16 cores. The current "All" chart still uses a 4-core per-host baseline because it is showing average host saturation.

This matches the model in `docs/metrics/time-travel.md`: average server line, real undivided query/merge bands, and per-host detail available through split views/tooltips.

## Ambiguity

The top chip can currently read like `CPUs: 4`, which is ambiguous in a cluster view. It can look like the whole selected cluster only has 4 CPUs, when it really means 4 effective CPUs per host.

The UI should make the baseline explicit, for example:

- `4/host · 4 hosts`
- `16 total · 4/host`
- `CPU baseline: avg host`

There may also be a metadata caching issue to check: the timeline service caches RAM/host-count metadata, so switching between a single host and "All" may preserve a stale `host_count = 1`. The CPU chip should be derived from metadata scoped to the current host selection.

## Follow-Up Options

1. Keep average-per-host as the default model.
   - Fix metadata cache scoping.
   - Rename the chip to make the baseline explicit.
   - Keep split view as the preferred way to diagnose one hot host.

2. Add a cluster-total CPU mode.
   - Sum CPU samples across selected hosts.
   - Use summed effective cores as the denominator.
   - Interpret `100%` as all selected hosts saturated.
   - This is better for fleet capacity, but less direct for seeing one overloaded node.

3. Offer both modes.
   - "Avg host saturation" for health/skew.
   - "Cluster total usage" for capacity planning.
   - Keep query and merge bands visually consistent with the selected mode.

## Questions To Resolve

- Should the default "All" view answer "is the average server hot?" or "how much total cluster CPU is being consumed?"
- Should query/merge bands stay undivided against the average host line, or be normalized when using an average baseline?
- Should the CPU chip use independent CPU host metadata instead of RAM-derived host metadata?
- Should the chart title or legend explicitly say `Avg host CPU` when in "All" mode?
