export interface DashboardPanel {
  /** Query reference - namespaced as 'Group#Query Name', or bare 'Query Name' for backward compat */
  queryName: string;
  /** Optional monitoring capability required to execute this panel. */
  requiredCapability?: string;
  /** If set, this panel starts a new collapsible section with this title.
   *  All subsequent panels until the next section belong to this section. */
  section?: string;
}

/** Dashboard group for visual categorization in the list view. */
export type DashboardGroup = 'ClickHouse' | 'Cloud Providers' | 'Grafana Imports' | 'Knowledge Base' | 'TraceHouse' | 'Custom';

/** Declares a global filter that the dashboard supports. The `param` value is injected into `{{drill_value:param}}` templates. */
export interface DashboardFilter {
  /** The template parameter name (e.g. 'tbl') - maps to {{drill_value:tbl}} in queries */
  param: string;
  /** Display label for the filter dropdown */
  label: string;
  /** SQL query that returns the list of values for the dropdown. First column is used as the value. */
  query: string;
  /**
   * Optional parent filter param. When set, this dropdown only shows options scoped to the
   * parent's current value — an option is kept if it equals the parent value or starts with
   * `parentValue + '.'` (e.g. a Table filter with dependsOn: 'db' whose options are
   * 'database.table' shows only the selected database's tables). Pure client-side filtering of
   * the already-fetched options; a now-invalid selection is cleared automatically.
   */
  dependsOn?: string;
}

export interface Dashboard {
  id: string;
  title: string;
  description?: string;
  /** Attribution URL for dashboards derived from external sources */
  source?: string;
  group?: DashboardGroup;
  /** Optional sub-category within the group (e.g. 'Merges', 'Storage') for visual grouping in the list view */
  category?: string;
  columns: 1 | 2 | 3 | 4;
  panels: DashboardPanel[];
  /** true for shipped defaults (user can clone but not delete the originals) */
  builtin?: boolean;
  /** Optional global filters shown as dropdowns in the dashboard header - values propagate to all panels */
  filters?: DashboardFilter[];
  /** SQL definitions owned by this dashboard, including drill/link targets. */
  queries?: string[];
}

