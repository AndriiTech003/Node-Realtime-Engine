import { describe, expect, it } from "vitest";
import { arr, bool, discriminated, int, json, lit, num, obj, opt, str, validate } from "../../src/validator.js";

describe("validator primitives", () => {
  it("validates strings with length and pattern", () => {
    const s = str({ min: 2, max: 4, pattern: /^[a-z]+$/ });
    expect(validate(s, "abc").ok).toBe(true);
    expect(validate(s, "a").ok).toBe(false);
    expect(validate(s, "abcde").ok).toBe(false);
    expect(validate(s, "AB").ok).toBe(false);
    expect(validate(s, 12).ok).toBe(false);
  });

  it("validates integers and numbers", () => {
    expect(validate(int({ min: 0 }), 3).ok).toBe(true);
    expect(validate(int({ min: 0 }), -1).ok).toBe(false);
    expect(validate(int(), 1.5).ok).toBe(false);
    expect(validate(int({ max: 10 }), 11).ok).toBe(false);
    expect(validate(num(), Number.NaN).ok).toBe(false);
    expect(validate(num(), Infinity).ok).toBe(false);
    expect(validate(num({ min: 1 }), 1.5).ok).toBe(true);
  });

  it("validates booleans and literals", () => {
    expect(validate(bool(), true).ok).toBe(true);
    expect(validate(bool(), 0).ok).toBe(false);
    expect(validate(lit("sub"), "sub").ok).toBe(true);
    expect(validate(lit("sub"), "pub").ok).toBe(false);
  });

  it("validates JSON values with a depth limit", () => {
    expect(validate(json(), { a: [1, "x", null, { b: true }] }).ok).toBe(true);
    expect(validate(json(), undefined).ok).toBe(false);
    expect(validate(json(), () => 1).ok).toBe(false);
    expect(validate(json(), { n: Number.NaN }).ok).toBe(false);
    expect(validate(json(), new Uint8Array(2)).ok).toBe(false);
    let deep: unknown = 1;
    for (let i = 0; i < 20; i++) deep = [deep];
    expect(validate(json({ maxDepth: 16 }), deep).ok).toBe(false);
    expect(validate(json({ maxDepth: 32 }), deep).ok).toBe(true);
  });

  it("validates objects with optional fields and strict mode", () => {
    const schema = obj({ a: int(), b: opt(str()) });
    expect(validate(schema, { a: 1 }).ok).toBe(true);
    expect(validate(schema, { a: 1, b: "x" }).ok).toBe(true);
    expect(validate(schema, { a: 1, b: 2 }).ok).toBe(false);
    expect(validate(schema, { b: "x" }).ok).toBe(false);
    expect(validate(schema, null).ok).toBe(false);
    expect(validate(schema, [1]).ok).toBe(false);
    expect(validate(schema, { a: 1, extra: true }).ok).toBe(true);
    expect(validate(obj({ a: int() }, { strict: true }), { a: 1, extra: true }).ok).toBe(false);
  });

  it("validates arrays and reports the failing path", () => {
    const result = validate(arr(obj({ x: int() }), { max: 3 }), [{ x: 1 }, { x: "no" }]);
    expect(result.ok).toBe(false);
    if (!result.ok) expect(result.error).toBe("$[1].x: expected integer");
    expect(validate(arr(int(), { max: 1 }), [1, 2]).ok).toBe(false);
  });

  it("dispatches discriminated unions by tag", () => {
    const schema = discriminated("t", { a: obj({ t: lit("a"), n: int() }), b: obj({ t: lit("b") }) });
    expect(validate(schema, { t: "a", n: 1 }).ok).toBe(true);
    expect(validate(schema, { t: "b" }).ok).toBe(true);
    expect(validate(schema, { t: "c" }).ok).toBe(false);
    expect(validate(schema, { t: "a" }).ok).toBe(false);
    expect(validate(schema, { t: "toString" }).ok).toBe(false);
  });
});
