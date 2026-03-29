/**
 * ConnectionForm - Form component for creating and testing ClickHouse connections
 * 
 * This component provides a form for entering connection details, testing connections,
 * and saving connection profiles.
 */

import React, { useState, useCallback, useEffect } from 'react';
import { useConnectionStore, defaultConnectionConfig } from '../../stores/connectionStore';
import type { ConnectionConfig, ConnectionConfigResponse, CredentialStorageMode } from '../../stores/connectionStore';
import { useProxyStore } from '../../stores/proxyStore';
import { useClusterStore } from '../../stores/clusterStore';

interface ConnectionFormProps {
  onClose?: () => void;
  onSuccess?: () => void;
}

export const ConnectionForm: React.FC<ConnectionFormProps> = ({ onClose, onSuccess }) => {
  const {
    profiles,
    editingProfileId,
    isLoading,
    isTestingConnection,
    error,
    testResult,
    createProfile,
    updateProfile,
    testConnection,
    clearError,
    clearTestResult,
    credentialStorageMode,
    setCredentialStorageMode,
  } = useConnectionStore();

  const editingProfile = editingProfileId ? profiles.find(p => p.id === editingProfileId) : null;
  const isEditing = !!editingProfile;

  // Form state
  const [profileName, setProfileName] = useState(editingProfile?.name ?? 'Local ClickHouse');
  const [config, setConfig] = useState<ConnectionConfig>(
    editingProfile
      ? {
          host: editingProfile.config.host,
          port: editingProfile.config.port,
          user: editingProfile.config.user,
          password: (editingProfile.config as ConnectionConfigResponse & { password?: string }).password ?? '',
          database: editingProfile.config.database,
          secure: editingProfile.config.secure,
          connect_timeout: editingProfile.config.connect_timeout,
          send_receive_timeout: editingProfile.config.send_receive_timeout,
          useCloudStickyRouting: editingProfile.config.useCloudStickyRouting,
        }
      : defaultConnectionConfig
  );
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [isClusterDropdownOpen, setIsClusterDropdownOpen] = useState(false);
  const [connectAfterSave, setConnectAfterSave] = useState(!isEditing);

  // Cluster state — only relevant when editing the active connection
  const clusterName = useClusterStore((s) => s.clusterName);
  const availableClusters = useClusterStore((s) => s.availableClusters);
  const switchCluster = useClusterStore((s) => s.switchCluster);

  // Proxy state
  const proxyBundled = useProxyStore((s) => s.bundled);
  const proxyEnabled = useProxyStore((s) => s.enabled);
  const proxyUrl = useProxyStore((s) => s.url);
  const proxyAvailable = useProxyStore((s) => s.available);
  const setProxyEnabled = useProxyStore((s) => s.setEnabled);
  const setProxyUrl = useProxyStore((s) => s.setUrl);
  const checkProxyAvailability = useProxyStore((s) => s.checkAvailability);

  // Check proxy availability when toggled on
  useEffect(() => {
    if (proxyEnabled) {
      checkProxyAvailability();
    }
  }, [proxyEnabled, proxyUrl, checkProxyAvailability]);

  // ESC key to close
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose?.();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [onClose]);

  // Update config field
  const updateConfig = useCallback(<K extends keyof ConnectionConfig>(
    field: K,
    value: ConnectionConfig[K]
  ) => {
    setConfig(prev => {
      const next = { ...prev, [field]: value };

      // When the host changes, auto-toggle secure and port defaults:
      // - local-like hosts → HTTP (port 8123, secure off)
      // - anything else    → HTTPS (port 8443, secure on)
      if (field === 'host') {
        const LOCAL_HOSTS = new Set(['', 'localhost', '127.0.0.1', 'host.docker.internal']);
        const isLocal = LOCAL_HOSTS.has((value as string).trim().toLowerCase());
        const wasLocal = LOCAL_HOSTS.has(prev.host.trim().toLowerCase());
        // Only auto-switch if the user hasn't manually changed port/secure from defaults
        const hasDefaultPort = prev.port === 8123 || prev.port === 8443;
        if (isLocal !== wasLocal && hasDefaultPort) {
          next.secure = !isLocal;
          next.port = isLocal ? 8123 : 8443;
        }
      }

      return next;
    });
    clearError();
    clearTestResult();
  }, [clearError, clearTestResult]);

  // Handle test connection
  const handleTestConnection = useCallback(async () => {
    await testConnection(config);
  }, [config, testConnection]);

  // Handle form submission
  const handleSubmit = useCallback(async (e: React.FormEvent) => {
    e.preventDefault();
    
    if (!profileName.trim()) {
      return;
    }

    try {
      if (isEditing && editingProfileId) {
        await updateProfile(editingProfileId, profileName.trim(), config);
      } else {
        await createProfile(profileName.trim(), config, true, connectAfterSave);
      }
      onSuccess?.();
      onClose?.();
    } catch (err) {
      console.warn('[ConnectionForm] Save failed:', err);
    }
  }, [profileName, config, connectAfterSave, isEditing, editingProfileId, createProfile, updateProfile, onSuccess, onClose]);

  // Handle cancel
  const handleCancel = useCallback(() => {
    clearError();
    clearTestResult();
    onClose?.();
  }, [clearError, clearTestResult, onClose]);

  // Input style - uses CSS variables for theme support
  const inputStyle: React.CSSProperties = {
    width: '100%',
    padding: '12px 16px',
    background: 'var(--bg-input)',
    border: '1px solid var(--border-input)',
    borderRadius: '8px',
    color: 'var(--text-primary)',
    fontSize: '14px',
    fontFamily: 'inherit',
    outline: 'none',
    transition: 'all 0.2s ease',
  };

  const labelStyle: React.CSSProperties = {
    display: 'block',
    fontSize: '11px',
    color: 'var(--text-muted)',
    marginBottom: '8px',
    textTransform: 'uppercase',
    letterSpacing: '1px',
  };

  return (
    <div 
      style={{
        background: 'var(--bg-modal)',
        border: '1px solid rgba(var(--accent-purple-rgb), 0.3)',
        borderRadius: '16px',
        boxShadow: 'var(--shadow-modal)',
        width: '480px',
        maxHeight: 'calc(100vh - 80px)',
        overflow: 'hidden',
        display: 'flex',
        flexDirection: 'column',
        margin: 'auto',
      }}
      onClick={(e) => e.stopPropagation()}
    >
      {/* Header */}
      <div style={{
        padding: '24px 32px 20px',
        borderBottom: '1px solid rgba(var(--accent-purple-rgb), 0.2)',
        background: 'var(--bg-card)',
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <div>
            <h2 style={{
              fontFamily: "'Orbitron', 'Rajdhani', monospace",
              fontSize: '20px',
              fontWeight: 600,
              color: 'var(--text-primary)',
              letterSpacing: '2px',
              textTransform: 'uppercase',
              marginBottom: '8px',
            }}>
              {isEditing ? 'Edit Connection' : 'New Connection'}
            </h2>
            <p style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
              {isEditing ? 'Update connection details' : 'Enter ClickHouse connection details'}
            </p>
          </div>
          <button 
            onClick={handleCancel}
            style={{
              background: 'transparent',
              border: 'none',
              color: 'var(--text-muted)',
              cursor: 'pointer',
              padding: '8px',
              borderRadius: '8px',
              transition: 'all 0.2s ease',
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = 'var(--bg-hover)';
              e.currentTarget.style.color = 'var(--text-primary)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = 'transparent';
              e.currentTarget.style.color = 'var(--text-muted)';
            }}
          >
            <svg width="20" height="20" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
      </div>

      {/* Form */}
      <form onSubmit={handleSubmit} style={{ flex: 1, overflowY: 'auto', padding: '24px 32px' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
          {/* Profile Name */}
          <div>
            <label style={labelStyle}>Connection Name *</label>
            <input
              type="text"
              value={profileName}
              onChange={(e) => setProfileName(e.target.value)}
              placeholder="My ClickHouse Server"
              style={inputStyle}
              onFocus={(e) => {
                e.currentTarget.style.borderColor = 'rgba(var(--accent-purple-rgb), 0.5)';
                e.currentTarget.style.boxShadow = '0 0 20px rgba(var(--accent-purple-rgb), 0.2)';
              }}
              onBlur={(e) => {
                e.currentTarget.style.borderColor = 'var(--border-input)';
                e.currentTarget.style.boxShadow = 'none';
              }}
              required
            />
          </div>

          {/* Host & Port Row */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 120px', gap: '16px' }}>
            <div>
              <label style={labelStyle}>Host *</label>
              <input
                type="text"
                value={config.host}
                onChange={(e) => updateConfig('host', e.target.value)}
                placeholder="localhost"
                style={inputStyle}
                onFocus={(e) => {
                  e.currentTarget.style.borderColor = 'rgba(var(--accent-purple-rgb), 0.5)';
                  e.currentTarget.style.boxShadow = '0 0 20px rgba(var(--accent-purple-rgb), 0.2)';
                }}
                onBlur={(e) => {
                  e.currentTarget.style.borderColor = 'var(--border-input)';
                  e.currentTarget.style.boxShadow = 'none';
                }}
                required
              />
            </div>
            <div>
              <label style={labelStyle}>Port</label>
              <input
                type="number"
                value={config.port}
                onChange={(e) => updateConfig('port', parseInt(e.target.value) || 8123)}
                min={1}
                max={65535}
                style={inputStyle}
                onFocus={(e) => {
                  e.currentTarget.style.borderColor = 'rgba(var(--accent-purple-rgb), 0.5)';
                  e.currentTarget.style.boxShadow = '0 0 20px rgba(var(--accent-purple-rgb), 0.2)';
                }}
                onBlur={(e) => {
                  e.currentTarget.style.borderColor = 'var(--border-input)';
                  e.currentTarget.style.boxShadow = 'none';
                }}
              />
            </div>
          </div>

          {/* Username & Password Row */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
            <div>
              <label style={labelStyle}>Username</label>
              <input
                type="text"
                value={config.user}
                onChange={(e) => updateConfig('user', e.target.value)}
                placeholder="default"
                style={inputStyle}
                onFocus={(e) => {
                  e.currentTarget.style.borderColor = 'rgba(var(--accent-purple-rgb), 0.5)';
                  e.currentTarget.style.boxShadow = '0 0 20px rgba(var(--accent-purple-rgb), 0.2)';
                }}
                onBlur={(e) => {
                  e.currentTarget.style.borderColor = 'var(--border-input)';
                  e.currentTarget.style.boxShadow = 'none';
                }}
              />
            </div>
            <div>
              <label style={labelStyle}>Password</label>
              <input
                type="password"
                value={config.password}
                onChange={(e) => updateConfig('password', e.target.value)}
                placeholder="••••••••"
                style={inputStyle}
                onFocus={(e) => {
                  e.currentTarget.style.borderColor = 'rgba(var(--accent-purple-rgb), 0.5)';
                  e.currentTarget.style.boxShadow = '0 0 20px rgba(var(--accent-purple-rgb), 0.2)';
                }}
                onBlur={(e) => {
                  e.currentTarget.style.borderColor = 'var(--border-input)';
                  e.currentTarget.style.boxShadow = 'none';
                }}
              />
            </div>
          </div>

          {/* Credential Storage Mode */}
          <div>
            <label style={labelStyle}>Credential Storage</label>
            <div style={{ display: 'flex', gap: '8px' }}>
              {([
                { mode: 'memory' as CredentialStorageMode, label: 'Memory only' },
                { mode: 'session' as CredentialStorageMode, label: 'Session' },
                { mode: 'persistent' as CredentialStorageMode, label: 'Remember' },
              ]).map(({ mode, label }) => (
                <button
                  key={mode}
                  type="button"
                  onClick={() => setCredentialStorageMode(mode)}
                  style={{
                    flex: 1,
                    padding: '8px 12px',
                    background: credentialStorageMode === mode
                      ? 'rgba(var(--accent-purple-rgb), 0.2)'
                      : 'var(--bg-input)',
                    border: credentialStorageMode === mode
                      ? '1px solid rgba(var(--accent-purple-rgb), 0.5)'
                      : '1px solid var(--border-input)',
                    borderRadius: '8px',
                    color: credentialStorageMode === mode
                      ? 'var(--accent-secondary)'
                      : 'var(--text-secondary)',
                    fontSize: '13px',
                    fontWeight: credentialStorageMode === mode ? 500 : 400,
                    cursor: 'pointer',
                    transition: 'all 0.2s ease',
                  }}
                >
                  {label}
                </button>
              ))}
            </div>
            <p style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '6px' }}>
              {credentialStorageMode === 'memory'
                ? 'Password is kept only in memory — lost on page refresh. Most secure, but you must re-enter it every time.'
                : credentialStorageMode === 'session'
                  ? 'Credentials are stored for this browser tab and cleared when you close it.'
                  : 'Credentials are saved in browser storage and persist across sessions.'}
            </p>
          </div>

          {/* Database */}
          <div>
            <label style={labelStyle}>Database</label>
            <input
              type="text"
              value={config.database}
              onChange={(e) => updateConfig('database', e.target.value)}
              placeholder="default"
              style={inputStyle}
              onFocus={(e) => {
                e.currentTarget.style.borderColor = 'rgba(var(--accent-purple-rgb), 0.5)';
                e.currentTarget.style.boxShadow = '0 0 20px rgba(var(--accent-purple-rgb), 0.2)';
              }}
              onBlur={(e) => {
                e.currentTarget.style.borderColor = 'var(--border-input)';
                e.currentTarget.style.boxShadow = 'none';
              }}
            />
          </div>

          {/* Cluster Override — shown when editing the active connection and clusters are detected */}
          {isEditing && availableClusters.length > 1 && (
            <div>
              <label style={labelStyle}>Cluster</label>
              <div style={{ position: 'relative' }}>
                <button
                  type="button"
                  onClick={() => setIsClusterDropdownOpen(!isClusterDropdownOpen)}
                  style={{
                    ...inputStyle,
                    cursor: 'pointer',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    textAlign: 'left',
                    borderColor: isClusterDropdownOpen ? 'rgba(var(--accent-purple-rgb), 0.5)' : 'var(--border-input)',
                    boxShadow: isClusterDropdownOpen ? '0 0 20px rgba(var(--accent-purple-rgb), 0.2)' : 'none',
                  }}
                >
                  <span>{clusterName ?? 'Single-node'}</span>
                  <span style={{ color: 'var(--text-muted)', display: 'flex', alignItems: 'center', gap: '8px' }}>
                    {clusterName && (() => {
                      const c = availableClusters.find(c => c.name === clusterName);
                      if (!c) return null;
                      return (
                        <span style={{ fontSize: '12px' }}>
                          {c.replicaCount > 1 ? `${c.replicaCount} replicas` : '1 node'}
                          {c.shardCount > 1 ? ` · ${c.shardCount} shards` : ''}
                        </span>
                      );
                    })()}
                    <svg
                      width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor"
                      strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
                      style={{ transform: isClusterDropdownOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s ease' }}
                    >
                      <path d="M6 9l6 6 6-6" />
                    </svg>
                  </span>
                </button>
                {isClusterDropdownOpen && (
                  <div style={{
                    position: 'absolute',
                    top: '100%',
                    left: 0,
                    right: 0,
                    marginTop: '4px',
                    background: 'var(--bg-modal, var(--bg-secondary, #1e1e2e))',
                    border: '1px solid rgba(var(--accent-purple-rgb), 0.3)',
                    borderRadius: '8px',
                    overflow: 'hidden',
                    zIndex: 10,
                    maxHeight: '200px',
                    overflowY: 'auto',
                    boxShadow: '0 8px 24px rgba(0,0,0,0.6)',
                  }}>
                    {availableClusters.map((c) => {
                      const isActive = clusterName === c.name;
                      return (
                        <div
                          key={c.name}
                          onClick={() => { switchCluster(c.name); setIsClusterDropdownOpen(false); }}
                          style={{
                            padding: '10px 16px',
                            cursor: 'pointer',
                            display: 'flex',
                            alignItems: 'center',
                            justifyContent: 'space-between',
                            background: isActive ? 'rgba(88,166,255,0.1)' : 'transparent',
                            borderBottom: '1px solid var(--border-input)',
                            transition: 'background 0.15s ease',
                          }}
                          onMouseEnter={(e) => { if (!isActive) e.currentTarget.style.background = 'rgba(255,255,255,0.05)'; }}
                          onMouseLeave={(e) => { e.currentTarget.style.background = isActive ? 'rgba(88,166,255,0.1)' : 'transparent'; }}
                        >
                          <span style={{ color: isActive ? '#58a6ff' : 'var(--text-primary)', fontSize: '14px', fontWeight: isActive ? 500 : 400 }}>
                            {c.name}
                          </span>
                          <span style={{ color: 'var(--text-muted)', fontSize: '12px' }}>
                            {c.replicaCount > 1 ? `${c.replicaCount} replicas` : '1 node'}
                            {c.shardCount > 1 ? ` · ${c.shardCount}sh` : ''}
                          </span>
                        </div>
                      );
                    })}
                    <div
                      onClick={() => { switchCluster(null); setIsClusterDropdownOpen(false); }}
                      style={{
                        padding: '10px 16px',
                        cursor: 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        background: clusterName === null ? 'rgba(128,128,128,0.1)' : 'transparent',
                        transition: 'background 0.15s ease',
                      }}
                      onMouseEnter={(e) => { if (clusterName !== null) e.currentTarget.style.background = 'rgba(255,255,255,0.05)'; }}
                      onMouseLeave={(e) => { e.currentTarget.style.background = clusterName === null ? 'rgba(128,128,128,0.1)' : 'transparent'; }}
                    >
                      <span style={{ color: clusterName === null ? 'var(--text-primary)' : 'var(--text-muted)', fontSize: '14px', fontWeight: clusterName === null ? 500 : 400 }}>
                        Single-node
                      </span>
                      <span style={{ color: 'var(--text-muted)', fontSize: '12px' }}>no fan-out</span>
                    </div>
                  </div>
                )}
              </div>
              <p style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '6px' }}>
                {clusterName
                  ? `Queries fan out across all replicas via clusterAllReplicas('${clusterName}', ...)`
                  : 'Queries run on this node only — no cluster fan-out'}
              </p>
            </div>
          )}

          {/* Secure Connection Toggle */}
          <label style={{ display: 'flex', alignItems: 'center', gap: '12px', cursor: 'pointer' }}>
            <div style={{
              width: '20px',
              height: '20px',
              borderRadius: '4px',
              border: config.secure ? '2px solid var(--accent-secondary)' : '2px solid var(--border-primary)',
              background: config.secure ? 'rgba(var(--accent-purple-rgb), 0.3)' : 'transparent',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              transition: 'all 0.2s ease',
            }}>
              {config.secure && (
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="var(--accent-secondary)" strokeWidth="3">
                  <polyline points="20 6 9 17 4 12" />
                </svg>
              )}
            </div>
            <input
              type="checkbox"
              checked={config.secure}
              onChange={(e) => updateConfig('secure', e.target.checked)}
              style={{ display: 'none' }}
            />
            <span style={{ fontSize: '13px', color: 'var(--text-secondary)' }}>
              Use TLS/SSL connection
            </span>
          </label>

          {/* Sticky Routing Toggle — only shown for ClickHouse Cloud hosts */}
          {/\.clickhouse\.cloud$/i.test(config.host) && (
            <div>
              <label style={{ display: 'flex', alignItems: 'center', gap: '12px', cursor: 'pointer' }}>
                <div style={{
                  width: '20px',
                  height: '20px',
                  borderRadius: '4px',
                  border: config.useCloudStickyRouting ? '2px solid var(--accent-secondary)' : '2px solid var(--border-primary)',
                  background: config.useCloudStickyRouting ? 'rgba(var(--accent-purple-rgb), 0.3)' : 'transparent',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  transition: 'all 0.2s ease',
                }}>
                  {config.useCloudStickyRouting && (
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="var(--accent-secondary)" strokeWidth="3">
                      <polyline points="20 6 9 17 4 12" />
                    </svg>
                  )}
                </div>
                <input
                  type="checkbox"
                  checked={config.useCloudStickyRouting ?? false}
                  onChange={(e) => updateConfig('useCloudStickyRouting', e.target.checked)}
                  style={{ display: 'none' }}
                />
                <span style={{ fontSize: '13px', color: 'var(--text-secondary)' }}>
                  Sticky routing (pin to one replica)
                </span>
              </label>
              <p style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '4px', marginLeft: '32px' }}>
                Uses *.sticky.* subdomain to prevent system table views from flipping between replicas.
                Requires replica-aware routing enabled on your CH Cloud service.
              </p>
            </div>
          )}

          {/* CORS Proxy Toggle — hidden in bundled mode (proxy is always active) */}
          {!proxyBundled && (
          <div>
            <label style={{ display: 'flex', alignItems: 'center', gap: '12px', cursor: 'pointer' }}>
              <div style={{
                width: '20px',
                height: '20px',
                borderRadius: '4px',
                border: proxyEnabled ? '2px solid var(--accent-secondary)' : '2px solid var(--border-primary)',
                background: proxyEnabled ? 'rgba(var(--accent-purple-rgb), 0.3)' : 'transparent',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                transition: 'all 0.2s ease',
              }}>
                {proxyEnabled && (
                  <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="var(--accent-secondary)" strokeWidth="3">
                    <polyline points="20 6 9 17 4 12" />
                  </svg>
                )}
              </div>
              <input
                type="checkbox"
                checked={proxyEnabled}
                onChange={(e) => setProxyEnabled(e.target.checked)}
                style={{ display: 'none' }}
              />
              <span style={{ fontSize: '13px', color: 'var(--text-secondary)' }}>
                Use CORS proxy (for remote servers) —{' '}
                <a
                  href="https://dmkskd.github.io/tracehouse/docs/guides/connecting"
                  target="_blank"
                  rel="noopener noreferrer"
                  style={{ fontSize: '11px', color: 'var(--accent-secondary)', textDecoration: 'underline' }}
                >
                  Connection guide
                </a>
              </span>
            </label>
            {proxyEnabled && (
              <div style={{ marginTop: '8px', marginLeft: '32px' }}>
                <input
                  type="text"
                  value={proxyUrl}
                  onChange={(e) => setProxyUrl(e.target.value)}
                  placeholder="http://localhost:8990/proxy"
                  style={{ ...inputStyle, fontSize: '12px', padding: '8px 12px' }}
                  onFocus={(e) => {
                    e.currentTarget.style.borderColor = 'rgba(var(--accent-purple-rgb), 0.5)';
                    e.currentTarget.style.boxShadow = '0 0 20px rgba(var(--accent-purple-rgb), 0.2)';
                  }}
                  onBlur={(e) => {
                    e.currentTarget.style.borderColor = 'var(--border-input)';
                    e.currentTarget.style.boxShadow = 'none';
                  }}
                />
                <p style={{
                  fontSize: '11px',
                  marginTop: '6px',
                  color: proxyAvailable === true
                    ? 'var(--color-success)'
                    : proxyAvailable === false
                      ? 'var(--color-error)'
                      : 'var(--text-muted)',
                }}>
                  {proxyAvailable === true
                    ? '✓ Proxy is running'
                    : proxyAvailable === false
                      ? '✗ Proxy not reachable — run: npx @tracehouse/proxy'
                      : 'Checking proxy...'}
                </p>
              </div>
            )}
          </div>
          )}

          {/* Advanced Settings Toggle */}
          <button
            type="button"
            onClick={() => setShowAdvanced(!showAdvanced)}
            style={{
              background: 'transparent',
              border: 'none',
              color: 'var(--accent-secondary)',
              fontSize: '12px',
              cursor: 'pointer',
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              padding: '4px 0',
            }}
          >
            <span style={{
              transform: showAdvanced ? 'rotate(90deg)' : 'rotate(0deg)',
              transition: 'transform 0.2s ease',
              display: 'inline-block',
            }}>
              ▶
            </span>
            Advanced Settings
          </button>

          {/* Advanced Settings */}
          {showAdvanced && (
            <div style={{
              paddingLeft: '16px',
              borderLeft: '2px solid rgba(var(--accent-purple-rgb), 0.3)',
              display: 'flex',
              flexDirection: 'column',
              gap: '16px',
            }}>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '16px' }}>
                <div>
                  <label style={labelStyle}>Connect Timeout (s)</label>
                  <input
                    type="number"
                    value={config.connect_timeout}
                    onChange={(e) => updateConfig('connect_timeout', parseInt(e.target.value) || 10)}
                    min={0}
                    style={inputStyle}
                    onFocus={(e) => {
                      e.currentTarget.style.borderColor = 'rgba(var(--accent-purple-rgb), 0.5)';
                      e.currentTarget.style.boxShadow = '0 0 20px rgba(var(--accent-purple-rgb), 0.2)';
                    }}
                    onBlur={(e) => {
                      e.currentTarget.style.borderColor = 'var(--border-input)';
                      e.currentTarget.style.boxShadow = 'none';
                    }}
                  />
                </div>
                <div>
                  <label style={labelStyle}>Send/Recv Timeout (s)</label>
                  <input
                    type="number"
                    value={config.send_receive_timeout}
                    onChange={(e) => updateConfig('send_receive_timeout', parseInt(e.target.value) || 30)}
                    min={0}
                    style={inputStyle}
                    onFocus={(e) => {
                      e.currentTarget.style.borderColor = 'rgba(var(--accent-purple-rgb), 0.5)';
                      e.currentTarget.style.boxShadow = '0 0 20px rgba(var(--accent-purple-rgb), 0.2)';
                    }}
                    onBlur={(e) => {
                      e.currentTarget.style.borderColor = 'var(--border-input)';
                      e.currentTarget.style.boxShadow = 'none';
                    }}
                  />
                </div>
              </div>
            </div>
          )}

          {/* Connect After Save Toggle */}
          {/* Connect After Save Toggle - only for new connections */}
          {!isEditing && (
          <label style={{ display: 'flex', alignItems: 'center', gap: '12px', cursor: 'pointer' }}>
            <div style={{
              width: '20px',
              height: '20px',
              borderRadius: '4px',
              border: connectAfterSave ? '2px solid var(--accent-secondary)' : '2px solid var(--border-primary)',
              background: connectAfterSave ? 'rgba(var(--accent-purple-rgb), 0.3)' : 'transparent',
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              transition: 'all 0.2s ease',
            }}>
              {connectAfterSave && (
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="var(--accent-secondary)" strokeWidth="3">
                  <polyline points="20 6 9 17 4 12" />
                </svg>
              )}
            </div>
            <input
              type="checkbox"
              checked={connectAfterSave}
              onChange={(e) => setConnectAfterSave(e.target.checked)}
              style={{ display: 'none' }}
            />
            <span style={{ fontSize: '13px', color: 'var(--text-secondary)' }}>
              Connect after saving
            </span>
          </label>
          )}

          {/* Test Result */}
          {testResult && (
            <div style={{
              padding: '16px',
              borderRadius: '8px',
              background: testResult.success ? 'rgba(var(--color-success-rgb), 0.1)' : 'rgba(var(--color-error-rgb), 0.1)',
              border: `1px solid ${testResult.success ? 'rgba(var(--color-success-rgb), 0.3)' : 'rgba(var(--color-error-rgb), 0.3)'}`,
            }}>
              <div style={{ display: 'flex', alignItems: 'flex-start', gap: '12px' }}>
                <span style={{
                  fontSize: '18px',
                  color: testResult.success ? 'var(--color-success)' : 'var(--color-error)',
                }}>
                  {testResult.success ? '✓' : '✗'}
                </span>
                <div>
                  <p style={{
                    fontWeight: 500,
                    color: testResult.success ? 'var(--color-success)' : 'var(--color-error)',
                    marginBottom: '4px',
                  }}>
                    {testResult.success ? 'Connection successful!' : 'Connection failed'}
                  </p>
                  {testResult.success && testResult.server_version && (
                    <p style={{ fontSize: '12px', color: 'rgba(var(--color-success-rgb), 0.8)' }}>
                      Server version: {testResult.server_version}
                      {testResult.latency_ms && ` (${testResult.latency_ms.toFixed(0)}ms)`}
                    </p>
                  )}
                  {!testResult.success && testResult.error_message && (
                    <div style={{ fontSize: '12px', color: 'rgba(var(--color-error-rgb), 0.8)' }}>
                      <p style={{ margin: 0 }}>{testResult.error_message}</p>
                      {testResult.error_type === 'mixed_content' && (
                        <p style={{ marginTop: '8px', color: 'var(--text-secondary)' }}>
                          Either enable TLS on your ClickHouse server, or use the CORS proxy which runs
                          server-side and is not subject to browser mixed-content restrictions.
                        </p>
                      )}
                      {testResult.error_type === 'cors' && (
                        <p style={{ marginTop: '8px', color: 'var(--text-secondary)' }}>
                          This is likely caused by CORS restrictions. Try enabling the proxy option above, or see the{' '}
                          <a
                            href="https://dmkskd.github.io/tracehouse/docs/guides/connecting"
                            target="_blank"
                            rel="noopener noreferrer"
                            style={{ color: 'var(--accent-secondary)', textDecoration: 'underline' }}
                          >
                            connection guide
                          </a>
                          {' '}for more options.
                        </p>
                      )}
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* Error Display */}
          {error && !testResult && (
            <div style={{
              padding: '16px',
              borderRadius: '8px',
              background: 'rgba(var(--color-error-rgb), 0.1)',
              border: '1px solid rgba(var(--color-error-rgb), 0.3)',
            }}>
              <div style={{ display: 'flex', alignItems: 'flex-start', gap: '12px' }}>
                <span style={{ fontSize: '18px', color: 'var(--color-error)' }}>✗</span>
                <p style={{ fontSize: '13px', color: 'rgba(var(--color-error-rgb), 0.8)' }}>{error}</p>
              </div>
            </div>
          )}
        </div>
      </form>

      {/* Actions */}
      <div style={{
        padding: '20px 32px',
        borderTop: '1px solid rgba(var(--accent-purple-rgb), 0.2)',
        background: 'var(--bg-card)',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
      }}>
        <button
          type="button"
          onClick={handleTestConnection}
          disabled={isTestingConnection || !config.host}
          style={{
            background: 'transparent',
            border: '1px solid rgba(var(--accent-purple-rgb), 0.4)',
            borderRadius: '8px',
            padding: '10px 20px',
            color: 'var(--accent-secondary)',
            fontSize: '13px',
            fontWeight: 500,
            cursor: isTestingConnection || !config.host ? 'not-allowed' : 'pointer',
            opacity: isTestingConnection || !config.host ? 0.5 : 1,
            transition: 'all 0.2s ease',
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
          }}
          onMouseEnter={(e) => {
            if (!isTestingConnection && config.host) {
              e.currentTarget.style.background = 'rgba(var(--accent-purple-rgb), 0.1)';
              e.currentTarget.style.borderColor = 'rgba(var(--accent-purple-rgb), 0.6)';
            }
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.background = 'transparent';
            e.currentTarget.style.borderColor = 'rgba(var(--accent-purple-rgb), 0.4)';
          }}
        >
          {isTestingConnection ? (
            <>
              <svg className="animate-spin" width="16" height="16" fill="none" viewBox="0 0 24 24">
                <circle style={{ opacity: 0.25 }} cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path style={{ opacity: 0.75 }} fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
              </svg>
              Testing...
            </>
          ) : (
            'Test Connection'
          )}
        </button>

        <div style={{ display: 'flex', gap: '12px' }}>
          <button
            type="button"
            onClick={handleCancel}
            style={{
              background: 'transparent',
              border: '1px solid var(--border-primary)',
              borderRadius: '8px',
              padding: '10px 20px',
              color: 'var(--text-tertiary)',
              fontSize: '13px',
              fontWeight: 500,
              cursor: 'pointer',
              transition: 'all 0.2s ease',
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = 'var(--bg-hover)';
              e.currentTarget.style.borderColor = 'var(--border-accent)';
              e.currentTarget.style.color = 'var(--text-primary)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = 'transparent';
              e.currentTarget.style.borderColor = 'var(--border-primary)';
              e.currentTarget.style.color = 'var(--text-tertiary)';
            }}
          >
            Cancel
          </button>
          <span title={!profileName.trim() ? 'Specify a connection name' : !config.host ? 'Specify a host' : undefined}>
          <button
            type="submit"
            form="connection-form"
            onClick={handleSubmit}
            disabled={isLoading || !profileName.trim() || !config.host}
            style={{
              background: 'linear-gradient(135deg, var(--accent-purple) 0%, var(--accent-secondary) 100%)',
              border: 'none',
              borderRadius: '8px',
              padding: '10px 24px',
              color: 'white',
              fontSize: '13px',
              fontWeight: 600,
              cursor: isLoading || !profileName.trim() || !config.host ? 'not-allowed' : 'pointer',
              opacity: isLoading || !profileName.trim() || !config.host ? 0.5 : 1,
              transition: 'all 0.2s ease',
              boxShadow: '0 4px 15px rgba(var(--accent-purple-rgb), 0.3)',
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
            }}
            onMouseEnter={(e) => {
              if (!isLoading && profileName.trim() && config.host) {
                e.currentTarget.style.boxShadow = '0 4px 25px rgba(var(--accent-purple-rgb), 0.5)';
                e.currentTarget.style.transform = 'translateY(-1px)';
              }
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.boxShadow = '0 4px 15px rgba(var(--accent-purple-rgb), 0.3)';
              e.currentTarget.style.transform = 'translateY(0)';
            }}
          >
            {isLoading ? (
              <>
                <svg className="animate-spin" width="16" height="16" fill="none" viewBox="0 0 24 24">
                  <circle style={{ opacity: 0.25 }} cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path style={{ opacity: 0.75 }} fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z" />
                </svg>
                Saving...
              </>
            ) : (
              isEditing ? 'Update Connection' : 'Save Connection'
            )}
          </button>
          </span>
        </div>
      </div>
    </div>
  );
};

export default ConnectionForm;
