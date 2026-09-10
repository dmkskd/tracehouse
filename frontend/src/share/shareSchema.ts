/**
 * A page's share schema is the explicit list of state that belongs in a copied URL.
 * State absent from a share schema is intentionally local/ephemeral.
 */
export type ShareParamType = 'string' | 'string[]' | 'number' | 'boolean';

export interface ShareParamDef<T = unknown> {
  type: ShareParamType;
  default?: T;
  /** Keep the coordinate in the URL even when it equals the product default. */
  persistDefault?: boolean;
}

export type ShareSchema = Record<string, ShareParamDef>;

export type ShareStateFromSchema<S extends ShareSchema> = {
  [K in keyof S]: S[K]['type'] extends 'number'
    ? number | undefined
    : S[K]['type'] extends 'boolean'
      ? boolean | undefined
      : S[K]['type'] extends 'string[]'
        ? string[] | undefined
        : string | undefined;
};

/** Marks every entry as a coordinate that must survive copy/paste and reload. */
export function defineShareSchema<const S extends ShareSchema>(schema: S): S {
  return schema;
}

function parseScalarParam(raw: string | null, def: ShareParamDef): unknown {
  if (raw === null || raw === '') return def.default;
  switch (def.type) {
    case 'number': {
      const value = Number(raw);
      return Number.isFinite(value) ? value : def.default;
    }
    case 'boolean': return raw === '1' || raw === 'true';
    default: return raw;
  }
}

export function parseShareParam(params: URLSearchParams, key: string, def: ShareParamDef): unknown {
  if (def.type === 'string[]') {
    const values = params.getAll(key).map(value => value.trim()).filter(Boolean);
    return values.length > 0 ? values : def.default;
  }
  return parseScalarParam(params.get(key), def);
}

export function serializeShareParam(value: unknown, def: ShareParamDef): string | string[] | null {
  if (value === undefined || value === null || value === '') return null;
  if (!def.persistDefault && value === def.default) return null;
  if (def.type === 'string[]') {
    const rawValues = Array.isArray(value) ? value : [value];
    const values = rawValues.map(item => String(item).trim()).filter(Boolean);
    return values.length > 0 ? values : null;
  }
  if (def.type === 'boolean') return value ? '1' : '0';
  return String(value);
}
