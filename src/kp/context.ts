/**
 * Ctx — สิ่งที่ handler ได้รับต่อ 1 message
 *
 *   c.topic / c.partition / c.offset / c.key   ข้อมูลตำแหน่งของ message
 *   c.headers<T>()                             headers เป็น string แล้ว
 *   c.payload<T>()                             value ที่ JSON.parse แล้ว (parse ครั้งเดียว)
 *   c.validate(Schema, data?)                  ตรวจด้วย schema → { ok, value } | { ok: false, issues }
 */
import type { StandardSchemaV1 } from "./standard-schema.js";

export type ValidateResult<T> =
  | { ok: true; value: T; issues?: undefined }
  | { ok: false; value?: undefined; issues: ReadonlyArray<StandardSchemaV1.Issue> };

/** รูปร่าง message ขั้นต่ำที่ Ctx ต้องการ (ตรงกับ eachMessage ของ KafkaJS API) */
export interface RawMessage {
  key?: Buffer | string | null;
  value: Buffer | string | null;
  offset: string;
  timestamp?: string;
  headers?: Record<string, Buffer | string | (Buffer | string)[] | undefined>;
}

const UNPARSED = Symbol("unparsed");

export class Ctx {
  readonly topic: string;
  readonly partition: number;
  readonly offset: string;
  readonly key: string | undefined;
  readonly timestamp: string | undefined;
  readonly raw: RawMessage;

  #payload: unknown = UNPARSED;
  #headers?: Record<string, string>;

  constructor(topic: string, partition: number, message: RawMessage) {
    this.topic = topic;
    this.partition = partition;
    this.offset = message.offset;
    this.key = message.key?.toString();
    this.timestamp = message.timestamp;
    this.raw = message;
  }

  /** headers ทั้งหมดเป็น string (header ที่ซ้ำหลายค่าจะเอาค่าสุดท้าย) */
  headers<T extends Record<string, string | undefined> = Record<string, string | undefined>>(): T {
    if (!this.#headers) {
      this.#headers = {};
      for (const [k, v] of Object.entries(this.raw.headers ?? {})) {
        if (v === undefined) continue;
        this.#headers[k] = (Array.isArray(v) ? v[v.length - 1] : v).toString();
      }
    }
    return this.#headers as T;
  }

  /** header ตัวเดียว */
  header(name: string): string | undefined {
    return this.headers()[name];
  }

  /**
   * value ที่ parse เป็น JSON แล้ว — `<T>` เป็นแค่ type hint ไม่ได้ตรวจจริง
   * ถ้าต้องการความถูกต้องให้ใช้ c.validate(Schema)
   * value ที่ไม่ใช่ JSON จะได้ string กลับไป
   */
  payload<T = unknown>(): T {
    if (this.#payload === UNPARSED) {
      const text = this.raw.value?.toString() ?? null;
      try {
        this.#payload = text === null ? null : JSON.parse(text);
      } catch {
        this.#payload = text;
      }
    }
    return this.#payload as T;
  }

  /**
   * ตรวจ data ด้วย Standard Schema (zod, valibot, arktype, ...)
   * ไม่ส่ง data → ตรวจ c.payload()
   * schema ต้อง validate แบบ sync ได้ (ถ้ามี async refine ให้ใช้ validateAsync)
   */
  validate<S extends StandardSchemaV1>(
    schema: S,
    data: unknown = this.payload(),
  ): ValidateResult<StandardSchemaV1.InferOutput<S>> {
    const result = schema["~standard"].validate(data);
    if (result instanceof Promise) {
      throw new TypeError("schema นี้ validate แบบ async — ใช้ await c.validateAsync() แทน");
    }
    return toResult(result);
  }

  async validateAsync<S extends StandardSchemaV1>(
    schema: S,
    data: unknown = this.payload(),
  ): Promise<ValidateResult<StandardSchemaV1.InferOutput<S>>> {
    return toResult(await schema["~standard"].validate(data));
  }
}

function toResult<T>(r: StandardSchemaV1.Result<T>): ValidateResult<T> {
  return r.issues ? { ok: false, issues: r.issues } : { ok: true, value: r.value };
}

/** แปลง issues เป็นข้อความอ่านง่าย เช่น "amount: Expected number, received string" */
export function formatIssues(issues: ReadonlyArray<StandardSchemaV1.Issue>): string {
  return issues
    .map((i) => {
      const path = (i.path ?? []).map((p) => String(typeof p === "object" ? p.key : p)).join(".");
      return path ? `${path}: ${i.message}` : i.message;
    })
    .join("; ");
}
