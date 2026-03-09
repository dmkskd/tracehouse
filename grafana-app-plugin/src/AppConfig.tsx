import React, { useState, useEffect } from 'react';
import { AppPluginMeta, PluginConfigPageProps } from '@grafana/data';
import { getBackendSrv } from '@grafana/runtime';
import {
  ALL_REFRESH_RATE_OPTIONS,
  type AppPluginSettings,
  resolvePluginConfig,
} from './types';

interface AppConfigProps extends PluginConfigPageProps<AppPluginMeta<AppPluginSettings>> {}

export function AppConfig({ plugin }: AppConfigProps) {
  const jsonData = plugin.meta.jsonData ?? {};
  const resolved = resolvePluginConfig(jsonData);

  const [allowedRates, setAllowedRates] = useState<Set<number>>(
    () => new Set(resolved.allowedRefreshRates)
  );
  const [defaultRate, setDefaultRate] = useState<number>(resolved.defaultRefreshRate);
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);

  // Reset "saved" indicator after a short delay
  useEffect(() => {
    if (!saved) return;
    const t = setTimeout(() => setSaved(false), 2000);
    return () => clearTimeout(t);
  }, [saved]);

  const toggleRate = (seconds: number) => {
    setAllowedRates(prev => {
      const next = new Set(prev);
      if (next.has(seconds)) {
        // Don't allow removing the last option
        if (next.size <= 1) return prev;
        next.delete(seconds);
        // If we just removed the current default, pick the first remaining
        if (seconds === defaultRate) {
          const remaining = ALL_REFRESH_RATE_OPTIONS.find(o => next.has(o.seconds));
          if (remaining) setDefaultRate(remaining.seconds);
        }
      } else {
        next.add(seconds);
      }
      return next;
    });
  };

  const save = async () => {
    setSaving(true);
    try {
      const settings: AppPluginSettings = {
        allowedRefreshRates: ALL_REFRESH_RATE_OPTIONS
          .filter(o => allowedRates.has(o.seconds))
          .map(o => o.seconds),
        defaultRefreshRate: defaultRate,
      };

      await getBackendSrv().post(`/api/plugins/${plugin.meta.id}/settings`, {
        jsonData: settings,
        enabled: plugin.meta.enabled,
        pinned: plugin.meta.pinned,
      });

      setSaved(true);
    } catch (err) {
      console.error('Failed to save plugin settings:', err);
    } finally {
      setSaving(false);
    }
  };

  const checkboxStyle: React.CSSProperties = {
    display: 'flex',
    alignItems: 'center',
    gap: 8,
    padding: '6px 12px',
    borderRadius: 6,
    cursor: 'pointer',
    fontSize: 14,
    fontFamily: 'system-ui, sans-serif',
  };

  return (
    <div style={{ padding: 32, maxWidth: 640, fontFamily: 'system-ui, sans-serif' }}>
      <h2 style={{ color: 'var(--text-primary, white)', marginBottom: 8 }}>
        TraceHouse Configuration
      </h2>
      <p style={{ color: 'var(--text-secondary, rgba(255,255,255,0.6))', marginBottom: 32, fontSize: 14 }}>
        Configure global settings that apply to all users of this plugin.
        These can be overridden per-user where applicable.
      </p>

      {/* Refresh Rates Section */}
      <div style={{
        background: 'var(--bg-secondary, rgba(255,255,255,0.04))',
        border: '1px solid var(--border-primary, rgba(255,255,255,0.1))',
        borderRadius: 8,
        padding: 24,
        marginBottom: 24,
      }}>
        <h3 style={{ color: 'var(--text-primary, white)', marginBottom: 4, fontSize: 16 }}>
          Allowed Refresh Rates
        </h3>
        <p style={{ color: 'var(--text-secondary, rgba(255,255,255,0.5))', fontSize: 13, marginBottom: 16 }}>
          Select which refresh intervals are available to users. Disabling aggressive
          rates (e.g. 1s) reduces load on your ClickHouse cluster.
        </p>

        <div style={{ display: 'flex', flexDirection: 'column', gap: 4, marginBottom: 20 }}>
          {ALL_REFRESH_RATE_OPTIONS.map(option => {
            const checked = allowedRates.has(option.seconds);
            const isOnlyOne = checked && allowedRates.size === 1;
            return (
              <label
                key={option.seconds}
                style={{
                  ...checkboxStyle,
                  background: checked ? 'rgba(168, 85, 247, 0.08)' : 'transparent',
                  opacity: isOnlyOne ? 0.6 : 1,
                }}
              >
                <input
                  type="checkbox"
                  checked={checked}
                  disabled={isOnlyOne}
                  onChange={() => toggleRate(option.seconds)}
                  style={{ accentColor: '#a855f7' }}
                />
                <span style={{ color: 'var(--text-primary, white)', minWidth: 40 }}>
                  {option.label}
                </span>
                {option.seconds === 1 && (
                  <span style={{
                    fontSize: 11,
                    color: '#f59e0b',
                    background: 'rgba(245, 158, 11, 0.1)',
                    padding: '2px 8px',
                    borderRadius: 4,
                  }}>
                    High load
                  </span>
                )}
                {option.seconds === 2 && (
                  <span style={{
                    fontSize: 11,
                    color: '#f59e0b',
                    background: 'rgba(245, 158, 11, 0.1)',
                    padding: '2px 8px',
                    borderRadius: 4,
                  }}>
                    Moderate load
                  </span>
                )}
              </label>
            );
          })}
        </div>

        <h3 style={{ color: 'var(--text-primary, white)', marginBottom: 4, fontSize: 16 }}>
          Default Refresh Rate
        </h3>
        <p style={{ color: 'var(--text-secondary, rgba(255,255,255,0.5))', fontSize: 13, marginBottom: 12 }}>
          The refresh rate selected by default when a user opens a page.
        </p>
        <select
          value={defaultRate}
          onChange={e => setDefaultRate(Number(e.target.value))}
          style={{
            background: 'var(--bg-tertiary, #1a1a2e)',
            color: 'var(--text-primary, white)',
            border: '1px solid var(--border-primary, rgba(255,255,255,0.15))',
            borderRadius: 6,
            padding: '8px 12px',
            fontSize: 14,
            fontFamily: 'system-ui, sans-serif',
          }}
        >
          {ALL_REFRESH_RATE_OPTIONS
            .filter(o => allowedRates.has(o.seconds))
            .map(o => (
              <option key={o.seconds} value={o.seconds}>{o.label}</option>
            ))}
        </select>
      </div>

      {/* Save Button */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        <button
          onClick={save}
          disabled={saving}
          style={{
            background: saving ? '#6b21a8' : '#a855f7',
            color: 'white',
            border: 'none',
            borderRadius: 6,
            padding: '10px 24px',
            fontSize: 14,
            fontWeight: 500,
            cursor: saving ? 'not-allowed' : 'pointer',
            fontFamily: 'system-ui, sans-serif',
          }}
        >
          {saving ? 'Saving...' : 'Save'}
        </button>
        {saved && (
          <span style={{ color: '#22c55e', fontSize: 13 }}>
            ✓ Settings saved
          </span>
        )}
      </div>
    </div>
  );
}
