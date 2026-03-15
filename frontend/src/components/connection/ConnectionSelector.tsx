/**
 * ConnectionSelector - Compact connection selector dropdown
 */

import React, { useEffect, useState, useCallback, useRef } from 'react';
import { createPortal } from 'react-dom';
import { useConnectionStore } from '../../stores/connectionStore';
import type { ConnectionProfile, ConnectionConfigResponse } from '../../stores/connectionStore';
import { useClusterStore } from '../../stores/clusterStore';
import { ConnectionForm } from './ConnectionForm';

const font = { fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif" };

export const ConnectionSelector: React.FC = () => {
  const {
    profiles,
    activeProfileId,
    isLoading,
    error,
    isConnectionFormOpen,
    fetchProfiles,
    deleteProfile,
    setActiveProfile,
    setConnectionFormOpen,
    clearError,
  } = useConnectionStore();

  const [isDropdownOpen, setIsDropdownOpen] = useState(false);
  const [deleteConfirmId, setDeleteConfirmId] = useState<string | null>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    fetchProfiles().catch((err) => {
      console.error('[ConnectionSelector] fetchProfiles failed:', err);
    });
  }, [fetchProfiles]);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsDropdownOpen(false);
        setDeleteConfirmId(null);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const handleSelectProfile = useCallback((profile: ConnectionProfile) => {
    // If password is required but missing (session/memory mode expired), open edit form to re-enter it
    if (needsPassword(profile)) {
      setIsDropdownOpen(false);
      setConnectionFormOpen(true, profile.id);
      return;
    }
    setActiveProfile(profile.id);
    setIsDropdownOpen(false);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [setActiveProfile, setConnectionFormOpen]);

  const handleDelete = useCallback(async (profileId: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (deleteConfirmId === profileId) {
      try {
        await deleteProfile(profileId);
        setDeleteConfirmId(null);
      } catch (err) {
        console.warn('[ConnectionSelector] Failed to delete profile:', err);
      }
    } else {
      setDeleteConfirmId(profileId);
    }
  }, [deleteConfirmId, deleteProfile]);

  const handleCancelDelete = useCallback((e: React.MouseEvent) => {
    e.stopPropagation();
    setDeleteConfirmId(null);
  }, []);

  const handleAddConnection = useCallback(() => {
    setIsDropdownOpen(false);
    setConnectionFormOpen(true);
  }, [setConnectionFormOpen]);

  const handleEditConnection = useCallback((profileId: string, e: React.MouseEvent) => {
    e.stopPropagation();
    setIsDropdownOpen(false);
    setConnectionFormOpen(true, profileId);
  }, [setConnectionFormOpen]);

  const selected = profiles.find(p => p.id === activeProfileId);
  const clusterName = useClusterStore((s) => s.clusterName);
  const replicaCount = useClusterStore((s) => s.replicaCount);

  /** True when a profile was saved with a password but currently has none (session/memory expired) */
  const needsPassword = (p: ConnectionProfile) => {
    const pw = (p.config as ConnectionConfigResponse & { password?: string }).password;
    return !!p.requiresPassword && !pw;
  };

  return (
    <>
      <div className="relative" ref={dropdownRef}>
        {/* Selector Button */}
        <button
          onClick={() => setIsDropdownOpen(!isDropdownOpen)}
          className="text-left rounded-md transition-colors flex items-center gap-1.5"
          style={{
            ...font,
            padding: '4px 8px',
            background: isDropdownOpen ? 'var(--bg-tertiary)' : 'transparent',
          }}
          onMouseEnter={(e) => { if (!isDropdownOpen) e.currentTarget.style.background = 'var(--bg-hover)'; }}
          onMouseLeave={(e) => { if (!isDropdownOpen) e.currentTarget.style.background = 'transparent'; }}
        >
          {selected && (
            <span className="rounded-full flex-shrink-0" style={{
              width: 6, height: 6,
              background: needsPassword(selected) ? '#f59e0b' : '#22c55e',
            }} />
          )}
          <span style={{ color: 'var(--text-primary)', fontSize: 12, fontWeight: 500 }}>
            {selected ? selected.name : 'No connection'}
          </span>
          {selected && needsPassword(selected) && (
            <span style={{
              fontSize: 9, fontWeight: 500, color: '#f59e0b',
              background: 'rgba(245,158,11,0.1)', padding: '1px 5px',
              borderRadius: 3, whiteSpace: 'nowrap',
            }}
            title="Password required — click to enter"
            >
              password required
            </span>
          )}
          <span style={{ color: 'var(--text-muted)', fontSize: 11, display: 'inline-flex', alignItems: 'center', gap: 3 }}>
            {selected ? (
              <>
                {selected.config.secure ? (
                  <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#22c55e" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0110 0v4"/></svg>
                ) : (
                  <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#ef4444" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 019.9-1"/></svg>
                )}
                {selected.config.host}:{selected.config.port}
              </>
            ) : ''}
          </span>
          {selected && clusterName && (
            <span style={{
              fontSize: 9,
              fontWeight: 500,
              color: '#58a6ff',
              background: 'rgba(88,166,255,0.1)',
              padding: '1px 5px',
              borderRadius: 3,
              whiteSpace: 'nowrap',
            }}
            title={`Cluster: ${clusterName} (${replicaCount} replica${replicaCount !== 1 ? 's' : ''})`}
            >
              ⊞ {clusterName}{replicaCount > 1 ? ` ×${replicaCount}` : ''}
            </span>
          )}
          <svg
            className={`w-3 h-3 transition-transform ${isDropdownOpen ? 'rotate-180' : ''}`}
            style={{ color: 'var(--text-muted)' }}
            fill="none" stroke="currentColor" viewBox="0 0 24 24"
          >
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
          </svg>
        </button>

        {/* Dropdown Menu */}
        {isDropdownOpen && (
          <div
            className="absolute right-0 mt-1 rounded-md shadow-lg z-50 overflow-hidden"
            style={{
              ...font,
              background: 'var(--bg-secondary)',
              border: '1px solid var(--border-primary)',
              minWidth: 200,
            }}
          >
            {/* Error */}
            {error && (
              <div className="px-2 py-1.5 flex items-center justify-between"
                style={{ background: 'rgba(248, 81, 73, 0.1)', borderBottom: '1px solid var(--border-primary)' }}>
                <span style={{ color: 'var(--accent-red)', fontSize: 11 }}>{error}</span>
                <button onClick={(e) => { e.stopPropagation(); clearError(); }}
                  className="p-0.5 rounded hover:opacity-80" style={{ color: 'var(--accent-red)' }}>
                  <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                  </svg>
                </button>
              </div>
            )}

            {/* Loading */}
            {isLoading && (
              <div className="px-2 py-1.5 flex items-center justify-center">
                <span className="animate-spin inline-block w-3 h-3 border-2 border-gray-400 border-t-gray-600 rounded-full" />
              </div>
            )}

            {/* Profile List */}
            {profiles.length > 0 && (
              <div className="max-h-48 overflow-y-auto">
                {profiles.map((profile) => {
                  const isActive = activeProfileId === profile.id;

                  if (deleteConfirmId === profile.id) {
                    return (
                      <div key={profile.id} className="px-2 py-1.5 flex items-center justify-between"
                        style={{ background: 'rgba(248, 81, 73, 0.06)', borderBottom: '1px solid var(--border-secondary)' }}>
                        <span style={{ fontSize: 11, color: 'var(--text-secondary)' }}>
                          Remove <span style={{ color: 'var(--text-primary)', fontWeight: 500 }}>{profile.name}</span>?
                        </span>
                        <div className="flex items-center gap-1 ml-2">
                          <button onClick={(e) => handleDelete(profile.id, e)}
                            className="px-1.5 py-0.5 rounded text-xs transition-colors"
                            style={{ color: 'var(--accent-red)', border: '1px solid var(--accent-red)', fontSize: 10 }}
                            onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--accent-red)'; e.currentTarget.style.color = '#fff'; }}
                            onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--accent-red)'; }}>
                            Yes
                          </button>
                          <button onClick={handleCancelDelete}
                            className="px-1.5 py-0.5 rounded text-xs transition-colors"
                            style={{ color: 'var(--text-muted)', border: '1px solid var(--border-primary)', fontSize: 10 }}
                            onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--bg-hover)'; }}
                            onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; }}>
                            No
                          </button>
                        </div>
                      </div>
                    );
                  }

                  return (
                    <div key={profile.id}
                      className="px-2 py-1 cursor-pointer transition-colors group flex items-center justify-between"
                      style={{ background: isActive ? 'rgba(139, 92, 246, 0.08)' : 'transparent', borderBottom: '1px solid var(--border-secondary)' }}
                      onClick={() => handleSelectProfile(profile)}
                      onMouseEnter={(e) => { if (!isActive) e.currentTarget.style.background = 'var(--bg-hover)'; }}
                      onMouseLeave={(e) => { e.currentTarget.style.background = isActive ? 'rgba(139, 92, 246, 0.08)' : 'transparent'; }}
                    >
                      <div className="flex items-center min-w-0 gap-1.5">
                        <span className="flex-shrink-0" style={{ width: 5 }}>
                          {isActive && <span className="block rounded-full" style={{
                            width: 5, height: 5,
                            background: needsPassword(profile) ? '#f59e0b' : '#22c55e',
                          }} />}
                        </span>
                        <span className="truncate" style={{ color: 'var(--text-primary)', fontSize: 12 }}>{profile.name}</span>
                        {needsPassword(profile) && (
                          <svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="#f59e0b" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                            <path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4" />
                          </svg>
                        )}
                        <span className="truncate" style={{ color: 'var(--text-muted)', fontSize: 10, display: 'inline-flex', alignItems: 'center', gap: 2 }}>
                          {profile.config.secure ? (
                            <svg width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="#22c55e" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0110 0v4"/></svg>
                          ) : (
                            <svg width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="#ef4444" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 019.9-1"/></svg>
                          )}
                          {profile.config.host}:{profile.config.port}
                        </span>
                      </div>
                      <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-all flex-shrink-0">
                        <button onClick={(e) => handleEditConnection(profile.id, e)}
                          className="p-0.5 rounded transition-all"
                          style={{ color: 'var(--text-muted)' }}
                          onMouseEnter={(e) => e.currentTarget.style.color = 'var(--accent-secondary)'}
                          onMouseLeave={(e) => e.currentTarget.style.color = 'var(--text-muted)'}
                          title="Edit">
                          <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
                            <path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7" />
                            <path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" />
                          </svg>
                        </button>
                        <button onClick={(e) => handleDelete(profile.id, e)}
                          className="p-0.5 rounded transition-all"
                          style={{ color: 'var(--text-muted)' }}
                          onMouseEnter={(e) => e.currentTarget.style.color = 'var(--accent-red)'}
                          onMouseLeave={(e) => e.currentTarget.style.color = 'var(--text-muted)'}
                          title="Remove">
                          <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                          </svg>
                        </button>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}

            {/* Empty State */}
            {profiles.length === 0 && !isLoading && (
              <div className="px-2 py-2 text-center">
                <span style={{ color: 'var(--text-muted)', fontSize: 11 }}>No saved connections</span>
              </div>
            )}

            {/* Add Connection */}
            <button onClick={handleAddConnection}
              className="w-full px-2 py-1.5 text-left flex items-center gap-1 transition-colors"
              style={{ color: 'var(--text-secondary)', borderTop: '1px solid var(--border-primary)', fontSize: 11 }}
              onMouseEnter={(e) => { e.currentTarget.style.background = 'var(--bg-hover)'; e.currentTarget.style.color = 'var(--text-primary)'; }}
              onMouseLeave={(e) => { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'var(--text-secondary)'; }}>
              <svg className="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
              </svg>
              Add connection
            </button>
          </div>
        )}
      </div>

      {/* Connection Form Modal */}
      {isConnectionFormOpen && createPortal(
        <div className="fixed inset-0 flex items-center justify-center"
          style={{ zIndex: 9999, background: 'var(--backdrop-overlay)', backdropFilter: 'var(--backdrop-blur)', padding: '40px' }}
          onClick={() => setConnectionFormOpen(false)}>
          <ConnectionForm onClose={() => setConnectionFormOpen(false)} onSuccess={() => fetchProfiles()} />
        </div>,
        document.body
      )}
    </>
  );
};

export default ConnectionSelector;
