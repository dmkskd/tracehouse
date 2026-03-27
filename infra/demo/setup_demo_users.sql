-- Demo/production user setup
--
-- Creates a read-only user for external access (via Caddy HTTPS).
-- The default user is configured in XML (clickhouse-users-demo/) and
-- restricted to localhost/Docker networks.
--
-- The read-only password is injected via the CLICKHOUSE_READONLY_PASSWORD
-- environment variable (set in .env or docker-compose environment).

-- ============================================================
-- Read-only settings profile
-- ============================================================
-- readonly=2: allows SET and temporary tables but no INSERT/ALTER/CREATE
-- allow_introspection_functions: needed for flamegraph / stack traces
CREATE SETTINGS PROFILE IF NOT EXISTS readonly_profile
  SETTINGS readonly = 2, allow_introspection_functions = 1;

-- ============================================================
-- Read-only user — accessible externally via HTTPS
-- ============================================================
-- Temporary password — setup_demo_passwords.sh overwrites it on first boot.
CREATE USER IF NOT EXISTS read_only
  IDENTIFIED BY 'changeme'
  SETTINGS PROFILE readonly_profile;

-- Grant SELECT on all databases (user tables + system tables)
GRANT SELECT ON *.* TO read_only;

-- Grant SHOW access for schema browsing
GRANT SHOW DATABASES, SHOW TABLES, SHOW COLUMNS ON *.* TO read_only;

-- Grant access to system tables the app reads
GRANT dictGet ON *.* TO read_only;
GRANT SYSTEM RELOAD DICTIONARY ON *.* TO read_only;
