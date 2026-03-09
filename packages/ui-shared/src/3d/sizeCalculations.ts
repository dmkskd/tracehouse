/**
 * Size calculation utilities for 3D part visualization
 *
 * These functions calculate proportional visual sizes for parts based on
 * their bytes_on_disk values, ensuring visual representation accurately
 * reflects the relative sizes of database parts.
 */

/**
 * Part data required for size calculation
 */
export interface PartSizeData {
  /** Unique identifier for the part */
  name: string;
  /** Size of the part in bytes on disk */
  bytes_on_disk: number;
}

/**
 * Result of size calculation for a part
 */
export interface PartVisualSize {
  /** Part name for identification */
  name: string;
  /** Original bytes on disk */
  bytes_on_disk: number;
  /** Proportional size (0 to 1, relative to total) */
  proportionalSize: number;
  /** Visual scale factor for 3D rendering */
  visualScale: number;
}

/**
 * Configuration for size calculation
 */
export interface SizeCalculationConfig {
  /** Minimum visual scale to ensure small parts are visible */
  minScale: number;
  /** Maximum visual scale to prevent oversized parts */
  maxScale: number;
  /** Base scale multiplier for visual representation */
  baseScale: number;
}

/**
 * Default configuration for size calculations
 */
export const defaultSizeConfig: SizeCalculationConfig = {
  minScale: 0.1,
  maxScale: 5.0,
  baseScale: 1.0,
};

/**
 * Calculate proportional sizes for a set of parts.
 *
 * For any set of parts, the visual size of each part is proportional
 * to its bytes_on_disk value relative to the total bytes of all displayed parts.
 *
 * @param parts - Array of parts with bytes_on_disk values
 * @param config - Optional configuration for size calculation
 * @returns Array of parts with calculated proportional and visual sizes
 */
export function calculatePartSizes(
  parts: PartSizeData[],
  config: SizeCalculationConfig = defaultSizeConfig
): PartVisualSize[] {
  if (parts.length === 0) {
    return [];
  }

  const totalBytes = parts.reduce((sum, part) => sum + part.bytes_on_disk, 0);

  if (totalBytes === 0) {
    const equalProportion = 1 / parts.length;
    return parts.map(part => ({
      name: part.name,
      bytes_on_disk: part.bytes_on_disk,
      proportionalSize: equalProportion,
      visualScale: config.minScale,
    }));
  }

  return parts.map(part => {
    const proportionalSize = part.bytes_on_disk / totalBytes;
    // Cube root gives more intuitive sizing for 3D volumes
    const rawScale = Math.cbrt(proportionalSize) * config.baseScale;
    const visualScale = Math.max(
      config.minScale,
      Math.min(config.maxScale, rawScale)
    );

    return {
      name: part.name,
      bytes_on_disk: part.bytes_on_disk,
      proportionalSize,
      visualScale,
    };
  });
}

/**
 * Verify that calculated sizes satisfy the proportionality property.
 *
 * @param results - Array of calculated part visual sizes
 * @param tolerance - Acceptable floating point tolerance (default 1e-10)
 * @returns true if proportionality is maintained, false otherwise
 */
export function verifyProportionality(
  results: PartVisualSize[],
  tolerance: number = 1e-10
): boolean {
  if (results.length === 0) {
    return true;
  }

  const totalProportion = results.reduce(
    (sum, part) => sum + part.proportionalSize,
    0
  );

  if (Math.abs(totalProportion - 1) > tolerance) {
    return false;
  }

  for (const part of results) {
    if (part.proportionalSize < 0) {
      return false;
    }
  }

  return true;
}

/**
 * Calculate the expected proportional size for a part given its bytes and total bytes.
 *
 * @param partBytes - Bytes on disk for the part
 * @param totalBytes - Total bytes across all parts
 * @returns Expected proportional size (0 to 1)
 */
export function calculateExpectedProportion(
  partBytes: number,
  totalBytes: number
): number {
  if (totalBytes === 0) {
    return 0;
  }
  return partBytes / totalBytes;
}
