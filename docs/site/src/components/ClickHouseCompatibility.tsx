/**
 * ClickHouseCompatibility
 *
 * Version picker for the client-facing compatibility page. Reads the same
 * VERSION_GATED_CAPABILITIES list the app uses to gate features at runtime,
 * so this page cannot drift from the product.
 *
 * The app evaluates that list against a probed server. Here we evaluate it
 * against a version the reader types, which is the only input available
 * before they connect.
 */

import React, { useMemo, useState } from 'react';
import { VERSION_GATED_CAPABILITIES } from '@tracehouse/core/types/version-gated-capabilities';

/** Supported floor, established by CH-COMPAT-016. */
const SUPPORTED_FLOOR = '23.8';

/** Versions offered as one-click presets. Matches the pinned test matrix. */
const PRESET_VERSIONS = ['23.8', '24.3', '24.8', '25.3', '25.8', '26.7'];

interface Parsed {
  major: number;
  minor: number;
}

function parseVersion(value: string): Parsed | null {
  const match = value.trim().match(/^(\d+)\.(\d+)/);
  if (!match) return null;
  return { major: Number(match[1]), minor: Number(match[2]) };
}

function meets(version: string, minimum: string): boolean {
  const a = parseVersion(version);
  const b = parseVersion(minimum);
  if (!a || !b) return false;
  if (a.major !== b.major) return a.major > b.major;
  return a.minor >= b.minor;
}

const styles: Record<string, React.CSSProperties> = {
  picker: {
    display: 'flex',
    flexWrap: 'wrap',
    alignItems: 'center',
    gap: 8,
    marginBottom: '1.5rem',
  },
  input: {
    width: 120,
    padding: '6px 10px',
    borderRadius: 6,
    border: '1px solid var(--ifm-color-emphasis-300)',
    background: 'var(--ifm-background-color)',
    color: 'var(--ifm-font-color-base)',
    fontFamily: 'var(--ifm-font-family-monospace)',
  },
  preset: {
    padding: '5px 10px',
    borderRadius: 6,
    border: '1px solid var(--ifm-color-emphasis-300)',
    background: 'transparent',
    color: 'var(--ifm-font-color-base)',
    cursor: 'pointer',
    fontSize: '0.85rem',
    fontFamily: 'var(--ifm-font-family-monospace)',
  },
  presetActive: {
    background: 'var(--ifm-color-primary)',
    borderColor: 'var(--ifm-color-primary)',
    color: '#fff',
  },
  verdict: {
    padding: '12px 16px',
    borderRadius: 8,
    marginBottom: '1.5rem',
    border: '1px solid var(--ifm-color-emphasis-300)',
  },
  list: { listStyle: 'none', paddingLeft: 0, margin: 0 },
  item: {
    padding: '10px 0',
    borderTop: '1px solid var(--ifm-color-emphasis-200)',
  },
  reason: {
    fontSize: '0.85rem',
    color: 'var(--ifm-color-emphasis-700)',
    marginTop: 2,
  },
  badge: {
    display: 'inline-block',
    padding: '1px 7px',
    borderRadius: 4,
    fontSize: '0.75rem',
    fontFamily: 'var(--ifm-font-family-monospace)',
    marginLeft: 8,
    background: 'var(--ifm-color-emphasis-200)',
  },
};

export default function ClickHouseCompatibility(): React.ReactElement {
  const [version, setVersion] = useState('24.3');

  const parsed = parseVersion(version);
  const belowFloor = parsed !== null && !meets(version, SUPPORTED_FLOOR);

  const { available, unavailable } = useMemo(() => {
    const sorted = [...VERSION_GATED_CAPABILITIES].sort((a, b) =>
      a.minVersion.localeCompare(b.minVersion, undefined, { numeric: true }),
    );
    return {
      available: sorted.filter(gate => meets(version, gate.minVersion)),
      unavailable: sorted.filter(gate => !meets(version, gate.minVersion)),
    };
  }, [version]);

  return (
    <div>
      <div style={styles.picker}>
        <label htmlFor="ch-version"><strong>Your ClickHouse version:</strong></label>
        <input
          id="ch-version"
          style={styles.input}
          value={version}
          onChange={event => setVersion(event.target.value)}
          placeholder="24.3"
          aria-label="ClickHouse version"
        />
        {PRESET_VERSIONS.map(preset => (
          <button
            key={preset}
            type="button"
            onClick={() => setVersion(preset)}
            style={{
              ...styles.preset,
              ...(parseVersion(version)?.major === parseVersion(preset)!.major
                && parseVersion(version)?.minor === parseVersion(preset)!.minor
                ? styles.presetActive
                : {}),
            }}
          >
            {preset}
          </button>
        ))}
      </div>

      {parsed === null ? (
        <div style={styles.verdict}>
          Enter a version like <code>24.3</code> or <code>25.3.14.14</code>.
        </div>
      ) : belowFloor ? (
        <div style={styles.verdict}>
          <strong>ClickHouse {version} is below the supported floor.</strong>
          <div style={styles.reason}>
            TraceHouse supports {SUPPORTED_FLOOR} and newer. Older servers are not
            tested and the app may fail in ways not listed here.
          </div>
        </div>
      ) : (
        <div style={styles.verdict}>
          <strong>
            ClickHouse {version}: {available.length} of{' '}
            {VERSION_GATED_CAPABILITIES.length} version-gated features available.
          </strong>
          <div style={styles.reason}>
            Capabilities not listed below are not version-gated and are
            supported on every version from {SUPPORTED_FLOOR}.
          </div>
        </div>
      )}

      {unavailable.length > 0 && (
        <>
          <h3>Not available on {version}</h3>
          <ul style={styles.list}>
            {unavailable.map(gate => (
              <li key={gate.id} style={styles.item}>
                <strong>{gate.label}</strong>
                <span style={styles.badge}>needs {gate.minVersion}+</span>
                <div style={styles.reason}>{gate.description}</div>
              </li>
            ))}
          </ul>
        </>
      )}

      {available.length > 0 && (
        <>
          <h3>Available on {version}</h3>
          <ul style={styles.list}>
            {available.map(gate => (
              <li key={gate.id} style={styles.item}>
                <strong>{gate.label}</strong>
                <span style={styles.badge}>{gate.minVersion}+</span>
              </li>
            ))}
          </ul>
        </>
      )}
    </div>
  );
}
