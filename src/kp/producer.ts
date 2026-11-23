/**
 * Producer สำหรับใช้ใน handler (ผ่าน Ctx)
 *
 * มี 2 แบบ:
 *   1. default  — ตั้ง config ตอนสร้าง KafkaApp ({ producer: {...} })
 *                 connect ตอน app.start() (ก่อน consumer เริ่ม) ใช้ร่วมกันทุก message
 *                 → c.getProducer().send(...)
 *   2. on-demand — ส่ง config ตอนใช้ → connect ครั้งแรกที่ send แล้ว cache ไว้ตาม config
 *                 (config เดิม = producer ตัวเดิม ไม่ connect ใหม่ทุก message)
 *                 → c.setProducer({...}).send(...)
 *
 * producer ทุกตัวเป็น client แยกจาก consumer (คนละ connection) และถูก disconnect ตอน app.stop()
 */
import Confluent, { type KafkaJS } from "@confluentinc/kafka-javascript";
import { connectWithTimeout, DEFAULT_CONNECT_TIMEOUT_MS } from "./connect.js";

type KafkaClient = KafkaJS.Kafka;
type Producer = KafkaJS.Producer;

export type ProducerConfig = KafkaJS.ProducerConstructorConfig;
export type RecordMetadata = KafkaJS.RecordMetadata;

/** เหมือน Message ของ KafkaJS แต่ value เป็น object ได้ (แปลงเป็น JSON ให้) */
export type ProduceMessage = Omit<KafkaJS.Message, "value"> & {
  value: Buffer | string | object | null;
};

export interface ProduceRecord {
  topic: string;
  messages: ProduceMessage[];
}

export interface KpProducer {
  send(record: ProduceRecord): Promise<RecordMetadata[]>;
}

export interface ProducerOptions {
  /**
   * config ของ Kafka client ใหม่ (brokers, sasl, ...) — ใช้ส่งไป cluster อื่น / user อื่น
   * ไม่ใส่ = ใช้ client ของ producer ที่ตั้งใน KafkaApp({ producer: { kafka } })
   */
  kafka?: KafkaJS.CommonConstructorConfig;
  /** config ของ producer เช่น { kafkaJS: { acks: 1, compression }, "linger.ms": 10 } */
  producer?: ProducerConfig;
}

function serialize(record: ProduceRecord): KafkaJS.ProducerRecord {
  return {
    topic: record.topic,
    messages: record.messages.map((m) => ({
      ...m,
      value:
        m.value === null || typeof m.value === "string" || Buffer.isBuffer(m.value)
          ? m.value
          : JSON.stringify(m.value),
    })),
  };
}

/** producer ที่ connect ตอนใช้ครั้งแรก (เรียก send พร้อมกันหลายตัวก็ connect ครั้งเดียว) */
class LazyProducer implements KpProducer {
  #producer?: Promise<Producer>;

  constructor(
    private readonly create: () => Producer,
    readonly label: string,
    private readonly connectTimeoutMs: number,
  ) {}

  connect(): Promise<Producer> {
    this.#producer ??= (async () => {
      const p = this.create();
      try {
        await connectWithTimeout(p, `producer (${this.label})`, this.connectTimeoutMs);
      } catch (err) {
        this.#producer = undefined; // ให้ลองใหม่ได้ครั้งหน้า
        throw err;
      }
      return p;
    })();
    return this.#producer;
  }

  async send(record: ProduceRecord): Promise<RecordMetadata[]> {
    const p = await this.connect();
    return p.send(serialize(record));
  }

  async disconnect(): Promise<void> {
    const pending = this.#producer;
    this.#producer = undefined;
    if (pending) await (await pending.catch(() => undefined))?.disconnect();
  }
}

export class ProducerManager {
  readonly #kafka?: KafkaClient;
  readonly #default?: LazyProducer;
  readonly #onDemand = new Map<string, LazyProducer>();

  /** kafka = client ของ producer (แยกจาก consumer) — undefined ถ้า app ไม่ได้ตั้ง producer */
  readonly #connectTimeoutMs: number;

  constructor(
    kafka: KafkaClient | undefined,
    defaultConfig: ProducerConfig | undefined,
    connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS,
  ) {
    this.#kafka = kafka;
    this.#connectTimeoutMs = connectTimeoutMs;
    // มี producer.kafka = มี producer default เสมอ (config ไม่ใส่ก็ได้ → ใช้ค่า default ของ lib)
    if (kafka) {
      this.#default = new LazyProducer(() => kafka.producer(defaultConfig ?? {}), "default", connectTimeoutMs);
    }
  }

  get hasDefault(): boolean {
    return this.#default !== undefined;
  }

  /** producer ที่ตั้งไว้ตอนสร้าง app */
  default(): KpProducer {
    if (!this.#default) {
      throw new Error(
        "ยังไม่ได้ตั้ง producer — ใส่ new KafkaApp({ producer: { kafka } }) หรือใช้ c.setProducer({ kafka: {...} })",
      );
    }
    return this.#default;
  }

  /** producer ตาม config (cache ตาม config: ค่าเดียวกัน = ตัวเดียวกัน) */
  get(options: ProducerOptions = {}): KpProducer {
    // หมายเหตุ: function ใน config (เช่น callback) ไม่ถูกนับใน key
    const key = JSON.stringify(options);
    let p = this.#onDemand.get(key);
    if (!p) {
      const client = options.kafka ? new Confluent.KafkaJS.Kafka(options.kafka) : this.#kafka;
      if (!client) {
        throw new Error("app ไม่ได้ตั้ง producer.kafka — ต้องส่ง c.setProducer({ kafka: {...} }) ระบุการเชื่อมต่อเอง");
      }
      p = new LazyProducer(() => client.producer(options.producer ?? {}), "on-demand", this.#connectTimeoutMs);
      this.#onDemand.set(key, p);
    }
    return p;
  }

  /** connect producer default ก่อน consumer เริ่ม — ถ้า config ผิดจะรู้ตั้งแต่ start */
  async start(): Promise<void> {
    await this.#default?.connect();
  }

  async stop(): Promise<void> {
    const all = [this.#default, ...this.#onDemand.values()].filter((p) => p !== undefined);
    this.#onDemand.clear();
    await Promise.allSettled(all.map((p) => p.disconnect()));
  }
}
