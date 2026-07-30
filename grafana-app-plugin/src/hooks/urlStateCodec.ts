export type UrlParamType = 'string' | 'string[]' | 'number' | 'boolean';

export interface UrlParamDef<T = unknown> {
  type: UrlParamType;
  default?: T;
}

export type UrlSchema = Record<string, UrlParamDef>;

export type UrlStateFromSchema<S extends UrlSchema> = {
  [K in keyof S]: S[K]['type'] extends 'number'
    ? number | undefined
    : S[K]['type'] extends 'boolean'
      ? boolean | undefined
      : S[K]['type'] extends 'string[]'
        ? string[] | undefined
        : string | undefined;
};

function parseScalarParam(raw: string | null, def: UrlParamDef): unknown {
  if (raw === null || raw === '') return def.default;
  switch (def.type) {
    case 'number': {
      const value = Number(raw);
      return Number.isFinite(value) ? value : def.default;
    }
    case 'boolean':
      return raw === '1' || raw === 'true';
    default:
      return raw;
  }
}

export function parseSearchParam(
  params: URLSearchParams,
  key: string,
  def: UrlParamDef,
): unknown {
  if (def.type === 'string[]') {
    const values = params.getAll(key)
      .map(value => value.trim())
      .filter(Boolean);
    return values.length > 0 ? values : def.default;
  }
  return parseScalarParam(params.get(key), def);
}

export function serializeParam(
  value: unknown,
  def: UrlParamDef,
): string | string[] | null {
  if (value === undefined || value === null || value === '') return null;
  if (value === def.default) return null;
  if (def.type === 'string[]') {
    const rawValues = Array.isArray(value) ? value : [value];
    const values = rawValues
      .map(item => String(item).trim())
      .filter(Boolean);
    return values.length > 0 ? values : null;
  }
  if (def.type === 'boolean') return value ? '1' : null;
  return String(value);
}
