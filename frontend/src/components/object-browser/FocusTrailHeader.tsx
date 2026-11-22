import { Fragment, useEffect, useRef, useState } from 'react';
import type { ObjectCard } from '@tracehouse/core/services/object-browser';
import { ObjectTypeIcon } from './ObjectCard';
import { visibleTrail, type FocusTrail } from './focusTrail';

/** Focus title: the trail from the origin to the current object, on one line. Earlier
 * steps are links back; a long middle folds into "…", whose menu lists the hidden steps.
 * Steps show the card title; the full name is on hover. */
export function FocusTrailHeader({ trail, cards, nameOf, onStep }: {
  trail: FocusTrail; cards: Map<string, ObjectCard>; nameOf: (key: string) => string; onStep: (key: string) => void;
}) {
  const [menu, setMenu] = useState(false);
  const gap = useRef<HTMLSpanElement>(null);
  useEffect(() => {
    if (!menu) return;
    const close = (event: MouseEvent) => { if (!gap.current?.contains(event.target as Node)) setMenu(false); };
    document.addEventListener('mousedown', close);
    return () => document.removeEventListener('mousedown', close);
  }, [menu]);
  const label = (key: string) => <>{cards.get(key) && <ObjectTypeIcon card={cards.get(key)!} />}<span className="step-name">{cards.get(key)?.title ?? nameOf(key)}</span></>;
  const back = (key: string) => <button className="focus-start" aria-label={`Back to ${nameOf(key)}`} title={`Back to ${nameOf(key)}`}
    onClick={() => { setMenu(false); onStep(key); }}>{label(key)}</button>;
  const current = trail.at(-1);
  return <h2><span className="focus-origin-label">Focus</span>
    {visibleTrail(trail).map((item, i) => <Fragment key={item.kind === 'step' ? item.key : 'gap'}>
      {i > 0 && <span className="focus-separator" aria-hidden="true">›</span>}
      {item.kind === 'gap'
        ? <span className="focus-gap" ref={gap}>
            <button className="focus-start" aria-label={`Show ${item.hidden.length} more steps`} aria-expanded={menu} title={item.hidden.map(nameOf).join(' › ')} onClick={() => setMenu(value => !value)}>…</button>
            {menu && <span className="focus-gap-menu" role="menu">{item.hidden.map(key => <Fragment key={key}>{back(key)}</Fragment>)}</span>}
          </span>
        : item.key === current
          ? <span className="focus-current" title={nameOf(item.key)}>{label(item.key)}</span>
          : back(item.key)}
    </Fragment>)}
  </h2>;
}
