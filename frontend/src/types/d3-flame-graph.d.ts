declare module 'd3-flame-graph' {
  import { Selection } from 'd3';

  interface FlameGraphOptions {
    width?: number;
    height?: number;
    cellHeight?: number;
    minFrameSize?: number;
    transitionDuration?: number;
    transitionEase?: (t: number) => number;
    inverted?: boolean;
    sort?: boolean;
    title?: string;
    tooltip?: boolean;
    selfValue?: boolean;
    label?: (d: { data: { name: string; value: number } }) => string;
    onClick?: (d: { data: { name: string; value: number } }) => void;
    setColorMapper?: (colorMapper: (d: { data: { name: string } }, originalColor: string) => string) => void;
  }

  interface FlameGraph {
    (selection: Selection<HTMLElement, unknown, null, undefined>): void;
    width(value: number): FlameGraph;
    height(value: number): FlameGraph;
    cellHeight(value: number): FlameGraph;
    minFrameSize(value: number): FlameGraph;
    transitionDuration(value: number): FlameGraph;
    transitionEase(value: (t: number) => number): FlameGraph;
    inverted(value: boolean): FlameGraph;
    sort(value: boolean): FlameGraph;
    title(value: string): FlameGraph;
    tooltip(value: boolean): FlameGraph;
    selfValue(value: boolean): FlameGraph;
    label(value: (d: { data: { name: string; value: number } }) => string): FlameGraph;
    onClick(value: (d: { data: { name: string; value: number } }) => void): FlameGraph;
    setColorMapper(colorMapper: (d: { data: { name: string } }, originalColor: string) => string): FlameGraph;
    resetZoom(): void;
    destroy(): void;
  }

  export function flamegraph(options?: FlameGraphOptions): FlameGraph;
}

declare module 'd3-flame-graph/dist/d3-flamegraph.css';
