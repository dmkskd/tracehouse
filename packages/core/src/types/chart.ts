/**
 * Chart visualization types for the analytics query language (`-- @chart type=...`).
 *
 * Single source of truth: the frontend meta-language parser and the Grafana
 * export both reference this, so adding a chart type only happens in one place.
 */
export type ChartType =
  | 'bar'
  | 'line'
  | 'pie'
  | 'area'
  | 'grouped_bar'
  | 'stacked_bar'
  | 'grouped_stacked_bar'
  | 'grouped_line'
  | 'radar';
