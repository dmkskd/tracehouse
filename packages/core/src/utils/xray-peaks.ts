/**
 * Headline peak statistics for the Query X-Ray.
 *
 * `max(d_cpu_cores)` is the wrong headline number. The largest interval in a
 * query is very often its last one: when a query ends, pooled threads detach
 * and merge their whole accumulated CPU time into that final interval, which
 * divided by a short dt yields a rate no steady-state execution reaches. The
 * SQL caps such intervals at the query's peak concurrency and flags them
 * (`rate_clamped`), so a capped sample is a ceiling, not a measurement.
 *
 * Which source you read decides whether you even see it:
 * `system.query_metric_log` writes a final row at query finish and therefore
 * captures the teardown, while `tracehouse.processes_history` cannot — the
 * query leaves `system.processes` before the next tick. Taking a plain maximum
 * therefore made the same query report very different peaks per source.
 *
 * So the headline reports the peak *sustained* rate, ignoring capped intervals.
 * The capped spikes remain on the chart, where they are marked.
 */

/** Minimal shape needed here; ProcessSample satisfies it. */
export interface ClampablePeakSample {
  d_cpu_cores: number;
  rate_clamped: boolean;
}

export interface PeakCores {
  /** The reported peak, in cores. 0 when there are no samples. */
  value: number;
  /** True when capped intervals existed and were left out of `value`. */
  excludedCapped: boolean;
  /**
   * True when every sample was capped, so `value` is itself a ceiling and no
   * sustained rate could be measured.
   */
  allCapped: boolean;
}

/**
 * Peak sustained CPU, ignoring intervals that hit their thread ceiling.
 *
 * Falls back to the overall maximum when every interval was capped: reporting 0
 * for a query that clearly used CPU would be worse than reporting a ceiling,
 * provided the caller discloses it (`allCapped`).
 */
export function peakSustainedCores(samples: readonly ClampablePeakSample[]): PeakCores {
  if (samples.length === 0) return { value: 0, excludedCapped: false, allCapped: false };

  let sustained = -Infinity;
  let overall = -Infinity;
  let capped = 0;
  for (const s of samples) {
    if (s.d_cpu_cores > overall) overall = s.d_cpu_cores;
    if (s.rate_clamped) {
      capped++;
      continue;
    }
    if (s.d_cpu_cores > sustained) sustained = s.d_cpu_cores;
  }

  if (capped === samples.length) {
    return { value: overall, excludedCapped: false, allCapped: true };
  }
  return { value: sustained, excludedCapped: capped > 0, allCapped: false };
}
