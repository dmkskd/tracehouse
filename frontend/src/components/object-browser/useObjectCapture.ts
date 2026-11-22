import { useCallback, useEffect, useRef, useState } from 'react';
import { collectObjects } from '@tracehouse/core/services/object-browser';
import type { Capture, Investigation } from '@tracehouse/core/services/object-browser';
import { useClickHouseServices } from '@tracehouse/ui-shared';

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
  const collect = useCallback(async (input: Omit<Investigation, 'connection'>, mode: 'catalog' | 'investigation' = 'investigation') => {
    if (!services) return;
    if (current.current) current.current.aborted = true;
    const cancellation = { aborted: false };current.current = cancellation;
    setLoading(true);setError(null);setCapture(null);
    try {
      const result = await collectObjects(services.adapter, { ...input, connection: contextFor(services.adapter) }, { cancellation, mode });
      if (!cancellation.aborted) setCapture(result);
    } catch (e) {
      if (!cancellation.aborted) setError(e instanceof Error ? e.message : String(e));
    } finally { if (!cancellation.aborted) setLoading(false); }
  }, [services]);
  const browse = useCallback(() => collect({ principal: '', database: '', table: '', operation: 'SELECT' }, 'catalog'), [collect]);
  return { browse, capture, error, loading, collect, connected: !!services };
}
