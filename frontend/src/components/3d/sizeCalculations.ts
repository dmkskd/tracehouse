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
  // Handle empty array
  if (parts.length === 0) {
    return [];
  }

  // Calculate total bytes across all parts
  const totalBytes = parts.reduce((sum, part) => sum + part.bytes_on_disk, 0);

  // Handle case where all parts have zero bytes
  if (totalBytes === 0) {
    // When total is zero, all parts get equal proportional size
    const equalProportion = 1 / parts.length;
    return parts.map(part => ({
      name: part.name,
      bytes_on_disk: part.bytes_on_disk,
      proportionalSize: equalProportion,
      visualScale: config.minScale,
    }));
  }

  // Calculate proportional sizes for each part
  return parts.map(part => {
    // Proportional size is the ratio of this part's bytes to total bytes
    const proportionalSize = part.bytes_on_disk / totalBytes;

    // Calculate visual scale based on proportional size
    // Scale is proportional to the cube root of the proportion for better visual representation
    // (since we're dealing with 3D volumes, cube root gives more intuitive sizing)
    const rawScale = Math.cbrt(proportionalSize) * config.baseScale;
    
    // Clamp the visual scale to configured bounds
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
 * This function can be used for validation and testing to ensure
 * the size calculation maintains the proportionality invariant.
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

  // Sum of all proportional sizes should equal 1 (within tolerance)
  const totalProportion = results.reduce(
    (sum, part) => sum + part.proportionalSize,
    0
  );

  if (Math.abs(totalProportion - 1) > tolerance) {
    return false;
  }

  // Each proportional size should be non-negative
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
 * This is a pure calculation function useful for testing.
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
