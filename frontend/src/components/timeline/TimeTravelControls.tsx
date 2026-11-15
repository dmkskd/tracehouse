import { TimeTravelServerSelector } from './TimeTravelServerSelector';
import { segmentsStyle } from './TimeTravelSegments.styles';
import { controlsStyle } from './TimeTravelControls.styles';

interface Props {
  hosts: readonly string[];
  selectedHosts: readonly string[];
  onSelectedHostsChange: (hosts: string[]) => void;
  perServerView: boolean;
  onPerServerViewChange: (split: boolean) => void;
  splitAvailable: boolean;
}

export function TimeTravelControls({
  hosts, selectedHosts, onSelectedHostsChange,
  perServerView, onPerServerViewChange, splitAvailable,
}: Props) {
  return (
    <div className={`tt-server-controls ${controlsStyle}`}>
      <TimeTravelServerSelector hosts={hosts} selectedHosts={selectedHosts} onChange={onSelectedHostsChange} />
      <span className="tt-controls-divider" aria-hidden="true" />
      <div className={`tt-segments ${segmentsStyle}`} role="group" aria-label="Chart layout">
        <button type="button" aria-pressed={!perServerView}
          onClick={() => onPerServerViewChange(false)}
          title="Aggregate resource usage across selected servers in one chart">
          Aggregated
        </button>
        <button type="button" aria-pressed={perServerView} disabled={!splitAvailable}
          onClick={() => onPerServerViewChange(true)}
          title={splitAvailable
            ? 'Show a separate chart for each selected server'
            : 'By Server is not available in experimental 3D modes'}>
          By Server
        </button>
      </div>
    </div>
  );
}
