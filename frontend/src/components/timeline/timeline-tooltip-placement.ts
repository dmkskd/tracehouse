/**
 * Hover tooltip placement for the Time Travel chart: beside the cursor, on the
 * side that hides the least of the highlighted bands, vertically centred on
 * the pointer and kept inside the plot.
 */

export type TooltipSide = 'right' | 'left';

/** A highlighted band's edges in SVG coordinates, one entry per sample in `xs`. */
export interface BandEdges {
  top: number[];
  bottom: number[];
}

export interface TooltipPlacementInput {
  cursorX: number;
  cursorY: number;
  width: number;
  height: number;
  /** SVG width; the tooltip keeps EDGE_MARGIN clear of both sides. */
  chartWidth: number;
  plotTop: number;
  plotBottom: number;
  /** Sample x positions, visible samples only. */
  xs: number[];
  bands: BandEdges[];
  /** Side chosen on the previous hover, kept while it is nearly as good. */
  previousSide: TooltipSide;
}

export interface TooltipPlacement {
  left: number;
  top: number;
  side: TooltipSide;
}

const EDGE_MARGIN = 12;
const CURSOR_GAP = 24;
const PLOT_INSET = 4;
/** Covering the crosshair hides the moment being described. */
const CROSSHAIR_PENALTY = 1e6;
/** Fraction of the tooltip's own area the other side must save before the tooltip switches. */
const SWITCH_THRESHOLD = 0.15;

/**
 * Two candidate boxes, one each side of the cursor, share the same `top`.
 * Each is scored by how much highlighted band it would hide, and the lower
 * score wins, unless the previous side is within SWITCH_THRESHOLD of it.
 */
export function placeTooltip(input: TooltipPlacementInput): TooltipPlacement {
  const { cursorX, cursorY, width, height, chartWidth, plotTop, plotBottom, xs, bands, previousSide } = input;

  // Candidate left edges, CURSOR_GAP from the cursor and pulled back inside the
  // chart. Near an edge the clamp can push a box back over the cursor; the
  // crosshair penalty below rules that box out.
  const clampLeft = (x: number) => Math.max(EDGE_MARGIN, Math.min(x, chartWidth - width - EDGE_MARGIN));
  const lefts: Record<TooltipSide, number> = {
    right: clampLeft(cursorX + CURSOR_GAP),
    left: clampLeft(cursorX - width - CURSOR_GAP),
  };

  // Vertically centred on the pointer and clamped inside the plot, so the box
  // glides with the cursor instead of jumping between above and below
  const top = Math.max(plotTop + PLOT_INSET, Math.min(cursorY - height / 2, plotBottom - height - PLOT_INSET));
  const bottom = top + height;

  // Score for a box at `left`: for every sample whose x falls inside the box,
  // add the vertical overlap between each band's [top, bottom] span and the
  // box's [top, bottom]. SVG y grows downward, so a band's `top` is the smaller y.
  // The sum is in px per sample, which equals px² only at one sample per px.
  const hiddenArea = (left: number) => {
    let area = 0;
    for (let i = 0; i < xs.length; i++) {
      if (xs[i] < left || xs[i] > left + width) continue;
      for (const band of bands) {
        const overlap = Math.min(band.bottom[i], bottom) - Math.max(band.top[i], top);
        area += Math.max(0, overlap);
      }
    }
    const coversCrosshair = cursorX >= left && cursorX <= left + width;
    return area + (coversCrosshair ? CROSSHAIR_PENALTY : 0);
  };

  const areas: Record<TooltipSide, number> = { right: hiddenArea(lefts.right), left: hiddenArea(lefts.left) };
  // Ties go to the right
  const best: TooltipSide = areas.left < areas.right ? 'left' : 'right';
  // Hysteresis: switching sides must save more than SWITCH_THRESHOLD of the
  // tooltip's own area, so it does not flip back and forth between bands
  const keepPrevious = areas[previousSide] <= areas[best] + SWITCH_THRESHOLD * width * height;
  const side = keepPrevious ? previousSide : best;
  return { left: lefts[side], top, side };
}
