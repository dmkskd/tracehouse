import { spinAnimation } from '../../styles/animations';

/** Shared indeterminate marker; the surrounding status provides the label. */
export function LoadingSpinner({ size = 12 }: { size?: number }) {
  return <span aria-hidden="true" style={{
    display: 'inline-block', boxSizing: 'border-box', width: size, height: size,
    flexShrink: 0, verticalAlign: 'middle',
    border: '2px solid var(--border-secondary)', borderTopColor: 'var(--color-info)',
    borderRadius: '50%', animation: `${spinAnimation} 1s linear infinite`,
  }} />;
}
