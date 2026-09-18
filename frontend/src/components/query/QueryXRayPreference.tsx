import type { QueryXRaySourcePreference } from '@tracehouse/core';
import { useQueryXRayPreference } from './query-xray-preference';
import { SettingsSection, SegmentedControl, type SegmentedOption } from '../settings/SettingsControls';

const SCOPE = 'Applies to Query X-Ray, comparison timelines, Time Travel zoom, and Series overlays. Merge X-Ray always uses the sampler.';

type Choice = QueryXRaySourcePreference | 'inherit';

export function QueryXRaySourceControl() {
  const { override, defaultSource, connectionId, setPreference } = useQueryXRayPreference();
  const disabled = !connectionId;

  // Inheriting 'auto' and pinning 'auto' resolve identically, so the explicit
  // pin is offered only where it can override an admin-pinned table.
  const options: SegmentedOption<Choice>[] = [
    defaultSource === 'auto'
      ? { value: 'inherit', label: 'auto', title: 'Pick the source per query: the metric log for finished queries, the sampler while one is running.' }
      : { value: 'inherit', label: 'default', title: `Follow the configured default (${defaultSource}).` },
    ...(defaultSource === 'auto' ? [] : [{ value: 'auto' as Choice, label: 'auto', title: 'Pick the source per query, ignoring the configured default.' }]),
    { value: 'processes_history', label: 'sampler', title: 'tracehouse.processes_history, installed by the sampler. 1s resolution, thread counts and progress counters.' },
    { value: 'query_metric_log', label: 'metric log', title: 'system.query_metric_log, built into ClickHouse 24.10+. No thread counts or progress counters.' },
  ];

  return (
    <SettingsSection label="X-Ray Source" hint={SCOPE}>
      <SegmentedControl
        ariaLabel="Query X-Ray source"
        columns={3}
        options={options}
        value={override ?? 'inherit'}
        disabled={disabled}
        title={disabled ? 'Connect to a server to choose a source' : undefined}
        onSelect={choice => setPreference(choice === 'inherit' ? undefined : choice)}
      />
    </SettingsSection>
  );
}
