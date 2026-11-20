import { useEffect, useRef, useState } from 'react';
import type { MemoryTimeline, OperationKind, OperationalEvent } from '@tracehouse/core';
import { ModalWrapper } from '../../shared/ModalWrapper';
import type { MetricMode } from '../timeline-constants';
import { PinnedResourcePreview } from './PinnedResourcePreview';
import { ResourceLoading } from './ResourceLoading';
import { ResourceExplorationModal } from './ResourceExplorationModal';
import type { ResourceDataLoader, ResourceOperationOpen } from './resource-exploration-model';
import { useResourceData } from './useResourceData';

/** One materialized snapshot owns both the pin and its expanded view. Repinning remounts it. */
export function PinnedResourceActivity({ data, atMs, mode, width, hiddenCategories, hostLabel, availableHosts, initialHosts, events, onViewEventDetails, onLoadResource, onOpenOperation, onUnpin, eventPopupOpen = false }: {
  data: MemoryTimeline; atMs: number; mode: MetricMode; width: number;
  hiddenCategories?: ReadonlySet<OperationKind>; hostLabel?: string; availableHosts?: readonly string[];
  initialHosts?: readonly string[];
  events?: OperationalEvent[];
  onViewEventDetails?: (event: OperationalEvent) => void;
  onLoadResource?: ResourceDataLoader; onOpenOperation?: ResourceOperationOpen; onUnpin?: () => void;
  eventPopupOpen?: boolean;
}) {
  const [original] = useState(data);
  const [originHosts] = useState(() => initialHosts ? [...initialHosts] : undefined);
  const [exploring, setExploring] = useState(false);
  const trigger = useRef<HTMLButtonElement>(null);
  const resource = useResourceData(original, mode, atMs, hostLabel ?? originHosts, onLoadResource);
  const close = () => { setExploring(false); trigger.current?.focus(); };
  useEffect(() => {
    if (exploring || eventPopupOpen || !onUnpin) return;
    const unpin = (event: KeyboardEvent) => {
      if (event.key === 'Escape' && !event.defaultPrevented && !document.querySelector('[role="dialog"][aria-modal="true"]')) {
        const target = event.target instanceof Element ? event.target : null;
        if (target?.closest('[role="combobox"][aria-expanded="true"], [aria-haspopup][aria-expanded="true"]')) return;
        event.preventDefault();
        event.stopPropagation();
        onUnpin();
      }
    };
    // Handle the pin before Grafana's bubbling keyboard shortcuts can consume Escape.
    // Dialogs, open event popups, and expanded controls handle their own dismissal.
    document.addEventListener('keydown', unpin, true);
    return () => document.removeEventListener('keydown', unpin, true);
  }, [exploring, eventPopupOpen, onUnpin]);
  return <>
    <PinnedResourcePreview data={resource.source} atMs={atMs} mode={mode} width={width} hiddenCategories={hiddenCategories}
      onExplore={() => setExploring(true)} buttonRef={trigger} summary={resource.summary} />
    {resource.loading && <div role="status" style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 6 }}>Loading interval samples…</div>}
    {resource.samplingWarning && <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 6 }}>{resource.samplingWarning}</div>}
    {resource.loadError && <div role="alert" style={{ fontSize: 10, marginTop: 6 }}>Could not load interval samples. Open exploration to retry.</div>}
    {exploring && (resource.loading ? <ResourceLoading onClose={close} /> : resource.loadError ? <ModalWrapper isOpen onClose={close} maxWidth={1100} height="85vh" ariaLabel="Resource usage at the selected time">
      <div style={{ padding: 16 }}>
        <button onClick={close} aria-label="Close resource exploration" style={{ float: 'right', cursor: 'pointer' }}>×</button>
        <p role="alert">Could not load resource samples: {resource.loadError}</p><button onClick={resource.retry}>Retry</button>
      </div>
    </ModalWrapper> : <ResourceExplorationModal data={resource.source} atMs={atMs} initialMode={mode} hostLabel={hostLabel}
      availableHosts={availableHosts} initialHosts={originHosts} events={events} onViewEventDetails={onViewEventDetails} hiddenCategories={hiddenCategories} onLoadResource={onLoadResource} onOpenOperation={onOpenOperation}
      initialResource={{ summary: resource.summary, snapshots: resource.snapshots }} initialDataLoaded={!!onLoadResource} initialSamplingWarning={resource.samplingWarning} onClose={close} />)}
  </>;
}
