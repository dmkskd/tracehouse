-- Create read-only user for testing non-write access
-- Credentials: read_only / read_only
--
-- This user can read all system tables the app needs but cannot
-- create/drop/alter tables, insert data, or kill queries.
--
-- Uses readonly=1 (strictest mode: no writes, no setting changes).
-- Has its own profile to avoid inheriting the default profile's
-- opentelemetry settings (which would fail under readonly=1).
--
-- Usage:
--   clickhouse client < infra/scripts/setup_read_only_user.sql
--   # or via HTTP:
--   curl http://localhost:8123 --data-binary @infra/scripts/setup_read_only_user.sql

-- NOTE: Default profile observability settings (opentelemetry, profiler, etc.)
-- are defined in XML via users.d/*.xml — do NOT set them here via SQL,
-- because the 'default' user/profile lives in read-only XML storage.

-- ============================================================
-- Read-only user
-- ============================================================
-- Create a dedicated settings profile for read-only access
CREATE SETTINGS PROFILE IF NOT EXISTS readonly_profile
  SETTINGS readonly = 1, allow_introspection_functions = 1;

-- Create user with the readonly profile (not default)
CREATE USER IF NOT EXISTS read_only
  IDENTIFIED BY 'read_only'
  SETTINGS PROFILE readonly_profile;

-- Grant SELECT on all databases (user tables + system tables)
GRANT SELECT ON *.* TO read_only;

-- Grant SHOW access for schema browsing
GRANT SHOW DATABASES, SHOW TABLES, SHOW COLUMNS ON *.* TO read_only;
