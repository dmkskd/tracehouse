import { TimeTravelLoading } from '../TimeTravelLoading';

const reads = [
  { label: 'Server resources', detail: 'Usage and capacity' },
  { label: 'Operation usage', detail: 'Queries, merges, and mutations' },
  { label: 'Measurements', detail: 'Available operation samples' },
];

/** Shared Time Travel loading presentation, without suggesting event data is being fetched. */
export function ResourceLoading({ onClose, delayMs = 0 }: { onClose: () => void; delayMs?: number }) {
  return <TimeTravelLoading title="Loading resource usage"
    description="Calculating resource usage for the selected servers and time."
    reads={reads} delayMs={delayMs} onClose={onClose} />;
}
