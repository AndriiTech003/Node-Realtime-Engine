export interface Schema<T> {
  readonly kind: string;
  check(value: unknown, path: string): string | null;
  readonly _type?: T;
}

export type Infer<S> = S extends Schema<infer T> ? T : never;

export type ValidationResult<T> = { ok: true; value: T } | { ok: false; error: string };

export function validate<T>(schema: Schema<T>, value: unknown): ValidationResult<T> {
  const error = schema.check(value, "$");
  if (error === null) return { ok: true, value: value as T };
  return { ok: false, error };
}

export interface StringOptions {
  min?: number;
  max?: number;
  pattern?: RegExp;
}

export function str(options: StringOptions = {}): Schema<string> {
  const min = options.min ?? 0;
  const max = options.max ?? Number.POSITIVE_INFINITY;
  const pattern = options.pattern;
  return {
    kind: "string",
    check(value, path) {
      if (typeof value !== "string") return `${path}: expected string`;
      if (value.length < min) return `${path}: shorter than ${min}`;
      if (value.length > max) return `${path}: longer than ${max}`;
      if (pattern !== undefined && !pattern.test(value)) return `${path}: does not match pattern`;
      return null;
    },
  };
}

export interface NumberOptions {
  min?: number;
  max?: number;
}

export function int(options: NumberOptions = {}): Schema<number> {
  const min = options.min ?? Number.MIN_SAFE_INTEGER;
  const max = options.max ?? Number.MAX_SAFE_INTEGER;
  return {
    kind: "int",
    check(value, path) {
      if (typeof value !== "number" || !Number.isInteger(value)) return `${path}: expected integer`;
      if (value < min) return `${path}: less than ${min}`;
      if (value > max) return `${path}: greater than ${max}`;
      return null;
    },
  };
}

export function num(options: NumberOptions = {}): Schema<number> {
  const min = options.min ?? Number.NEGATIVE_INFINITY;
  const max = options.max ?? Number.POSITIVE_INFINITY;
  return {
    kind: "number",
    check(value, path) {
      if (typeof value !== "number" || !Number.isFinite(value)) return `${path}: expected number`;
      if (value < min) return `${path}: less than ${min}`;
      if (value > max) return `${path}: greater than ${max}`;
      return null;
    },
  };
}

export function bool(): Schema<boolean> {
  return {
    kind: "boolean",
    check(value, path) {
      return typeof value === "boolean" ? null : `${path}: expected boolean`;
    },
  };
}

export function lit<const L extends string | number | boolean>(literal: L): Schema<L> {
  return {
    kind: "literal",
    check(value, path) {
      return value === literal ? null : `${path}: expected ${JSON.stringify(literal)}`;
    },
  };
}

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

export interface JsonOptions {
  maxDepth?: number;
}

function checkJson(value: unknown, depth: number, maxDepth: number): boolean {
  if (value === null) return true;
  switch (typeof value) {
    case "boolean":
    case "string":
      return true;
    case "number":
      return Number.isFinite(value);
    case "object": {
      if (depth >= maxDepth) return false;
      if (Array.isArray(value)) {
        for (const item of value) if (!checkJson(item, depth + 1, maxDepth)) return false;
        return true;
      }
      if (value instanceof Uint8Array) return false;
      for (const key in value) {
        if (!checkJson((value as Record<string, unknown>)[key], depth + 1, maxDepth)) return false;
      }
      return true;
    }
    default:
      return false;
  }
}

export function json(options: JsonOptions = {}): Schema<JsonValue> {
  const maxDepth = options.maxDepth ?? 16;
  return {
    kind: "json",
    check(value, path) {
      if (value === undefined) return `${path}: required`;
      return checkJson(value, 0, maxDepth) ? null : `${path}: expected JSON value (max depth ${maxDepth})`;
    },
  };
}

export interface OptionalSchema<T> extends Schema<T | undefined> {
  readonly optional: true;
}

export function opt<T>(schema: Schema<T>): OptionalSchema<T> {
  return {
    kind: `optional<${schema.kind}>`,
    optional: true,
    check(value, path) {
      if (value === undefined) return null;
      return schema.check(value, path);
    },
  };
}

type Shape = Record<string, Schema<unknown>>;

type OptionalKeys<S extends Shape> = {
  [K in keyof S]: S[K] extends OptionalSchema<unknown> ? K : never;
}[keyof S];

type RequiredKeys<S extends Shape> = Exclude<keyof S, OptionalKeys<S>>;

export type ObjectOf<S extends Shape> = { [K in RequiredKeys<S>]: Infer<S[K]> } & {
  [K in OptionalKeys<S>]?: Infer<S[K]>;
};

type Simplify<T> = { [K in keyof T]: T[K] } & {};

export interface ObjectOptions {
  strict?: boolean;
}

export function obj<S extends Shape>(shape: S, options: ObjectOptions = {}): Schema<Simplify<ObjectOf<S>>> {
  const keys = Object.keys(shape);
  const strict = options.strict ?? false;
  return {
    kind: "object",
    check(value, path) {
      if (typeof value !== "object" || value === null || Array.isArray(value)) return `${path}: expected object`;
      const record = value as Record<string, unknown>;
      for (const key of keys) {
        const schema = shape[key] as Schema<unknown>;
        const error = schema.check(record[key], `${path}.${key}`);
        if (error !== null) return error;
      }
      if (strict) {
        for (const key in record) {
          if (!(key in shape)) return `${path}.${key}: unexpected field`;
        }
      }
      return null;
    },
  };
}

export function arr<T>(item: Schema<T>, options: { max?: number } = {}): Schema<T[]> {
  const max = options.max ?? Number.POSITIVE_INFINITY;
  return {
    kind: "array",
    check(value, path) {
      if (!Array.isArray(value)) return `${path}: expected array`;
      if (value.length > max) return `${path}: more than ${max} items`;
      for (let i = 0; i < value.length; i++) {
        const error = item.check(value[i], `${path}[${i}]`);
        if (error !== null) return error;
      }
      return null;
    },
  };
}

export function discriminated<K extends string, M extends Record<string, Schema<unknown>>>(
  key: K,
  mapping: M,
): Schema<Infer<M[keyof M]>> {
  return {
    kind: "union",
    check(value, path) {
      if (typeof value !== "object" || value === null || Array.isArray(value)) return `${path}: expected object`;
      const tag = (value as Record<string, unknown>)[key];
      if (typeof tag !== "string" || !Object.prototype.hasOwnProperty.call(mapping, tag)) {
        return `${path}.${key}: unknown type`;
      }
      return (mapping[tag] as Schema<unknown>).check(value, path);
    },
  };
}
