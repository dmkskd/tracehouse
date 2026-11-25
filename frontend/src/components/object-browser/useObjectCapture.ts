import { useCallback, useEffect, useRef, useState } from 'react';
import { collectObjects, notGrantedSources } from '@tracehouse/core/services/object-browser';
import type { Capture, Investigation } from '@tracehouse/core/services/object-browser';
import { useClickHouseServices } from '@tracehouse/ui-shared';
import { useMonitoringCapabilitiesStore } from '../../stores/monitoringCapabilitiesStore';

/** Manual capture only; request lifetime is bounded by route and adapter identity. */
let nextContext = 0;
const contexts = new WeakMap<object, string>();
function contextFor(adapter: object): string {
  let scope = contexts.get(adapter);
  if (!scope) { scope = `adapter-context:${Date.now()}:${++nextContext}`;contexts.set(adapter, scope); }
  return scope;
}
export function useObjectCapture() {
  const services = useClickHouseServices();
  const [capture, setCapture] = useState<Capture | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const current = useRef<{ aborted: boolean } | null>(null);
  useEffect(() => {
    setCapture(null);setError(null);setLoading(false);
    return () => { if (current.current) current.current.aborted = true; };
  }, [services?.adapter]);
  const collect = useCallback(async (input: Omit<Investigation, 'connection'>, mode: 'catalog' | 'investigation' = 'investigation', systemTables = false) => {
    if (!services) return;
    if (current.current) current.current.aborted = true;
    const cancellation = { aborted: false };current.current = cancellation;
    setLoading(true);setError(null);setCapture(null);
    try {
      // Sources the capability probe found without SELECT are skipped; before the probe finishes every source is read.
      const { capabilities, probeStatus } = useMonitoringCapabilitiesStore.getState();
      const notGranted = probeStatus === 'done' && capabilities ? notGrantedSources(capabilities.capabilities) : [];
      const result = await collectObjects(services.adapter, { ...input, connection: contextFor(services.adapter) }, { cancellation, mode, notGranted, systemTables });
      if (!cancellation.aborted) setCapture(result);
    } catch (e) {
      if (!cancellation.aborted) setError(e instanceof Error ? e.message : String(e));
    } finally { if (!cancellation.aborted) setLoading(false); }
  }, [services]);
  const browse = useCallback((systemTables = false) => collect({ principal: '', database: '', table: '', operation: 'SELECT' }, 'catalog', systemTables), [collect]);
  /** Shows a capture collected earlier in this session, cancelling any collection in flight. */
  const show = useCallback((earlier: Capture) => {
    if (current.current) current.current.aborted = true;
    current.current = null;setError(null);setLoading(false);setCapture(earlier);
  }, []);
  return { browse, capture, error, loading, collect, show, connected: !!services };
}
