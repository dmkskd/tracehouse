import React from 'react';

/**
 * Last line of defence around the notebook renderer.
 *
 * validateNotebook() catches every contract violation we know how to name, and
 * is where errors should normally surface. This exists for the ones we do not:
 * the renderer walks agent-authored data, and a shape that passes validation
 * but still breaks a chart primitive should cost the user this panel, not the
 * whole route.
 */
interface Props {
  children: React.ReactNode;
  /** Shown above the error so the user knows which notebook failed. */
  title?: string;
  onReset?: () => void;
}

interface State {
  error: Error | null;
}

export class NotebookErrorBoundary extends React.Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidUpdate(prev: Props) {
    // A new notebook deserves a fresh attempt; without this the boundary stays
    // latched on the previous document's failure.
    if (prev.children !== this.props.children && this.state.error) {
      this.setState({ error: null });
    }
  }

  render() {
    const { error } = this.state;
    if (!error) return this.props.children;

    return (
      <div className="card" style={{ margin: 24, padding: 20, borderLeft: '2px solid var(--accent-red, #ff6b6b)' }}>
        <h2 style={{ margin: '0 0 8px', fontSize: 15, color: 'var(--text-primary)' }}>
          This notebook could not be rendered
        </h2>
        {this.props.title && (
          <p style={{ margin: '0 0 10px', color: 'var(--text-secondary)', fontSize: 13 }}>{this.props.title}</p>
        )}
        <pre style={{
          margin: 0, padding: 12, overflow: 'auto',
          background: 'var(--bg-primary)', border: '1px solid var(--border-primary)', borderRadius: 6,
          color: 'var(--text-muted)', fontSize: 11, whiteSpace: 'pre-wrap',
        }}>
          {error.message}
        </pre>
        {this.props.onReset && (
          <button
            onClick={() => { this.setState({ error: null }); this.props.onReset?.(); }}
            style={{
              marginTop: 12, padding: '7px 10px', borderRadius: 6, cursor: 'pointer',
              border: '1px solid var(--border-primary)', background: 'var(--bg-secondary)',
              color: 'var(--text-secondary)', fontSize: 11,
            }}
          >
            Close notebook
          </button>
        )}
      </div>
    );
  }
}
