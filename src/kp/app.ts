/**
 * KafkaApp — รวม controller ทั้งหมดเข้า consumer ตัวเดียว แล้ว route message ตาม topic
 *
 *   const app = new KafkaApp({
 *     controllers: [OrderController],
 *     consumer: { kafka: consumerConn, groupId: "billing" },
 *     producer: { kafka: producerConn, config: { kafkaJS: { idempotent: true } } },
 *   });
 *   await app.start();
 *
 * Error: ถ้า handler throw → เรียก onError(err, ctx)
 *   - onError คืนค่าปกติ   → ถือว่าจัดการแล้ว commit offset ไปต่อ (default: log แล้วข้าม)
 *   - onError throw ต่อ     → ไม่ commit, Kafka จะส่ง message นี้มาใหม่ (at-least-once / retry)
 *
 * consumer กับ producer ใช้การเชื่อมต่อแยกกัน (คนละ Kafka client / user / clientId)
 * Producer: ใส่ { producer: { kafka, config } } → app สร้าง producer default ให้ตอน start
 *   handler ใช้ผ่าน c.getProducer() / c.setProducer({...}) — ดู producer.ts
 */
import Confluent, { type KafkaJS } from "@confluentinc/kafka-javascript";
import { Ctx } from "./context.js";
import { collectHandlers, isController, type Handler } from "./decorators.js";
import { connectWithTimeout, DEFAULT_CONNECT_TIMEOUT_MS } from "./connect.js";
import { ProducerManager, type ProducerConfig } from "./producer.js";

type KafkaClient = KafkaJS.Kafka;
type KafkaConsumer = KafkaJS.Consumer;

type Class<T = object> = new () => T;

/** Kafka client ที่สร้างไว้แล้ว หรือ config สำหรับสร้างใหม่ (brokers, sasl, clientId, ...) */
export type KafkaConnection = KafkaClient | KafkaJS.CommonConstructorConfig;

export interface ConsumerOptions {
  /** การเชื่อมต่อของ consumer — แยกจาก producer (คนละ client / user / clientId ได้) */
  kafka: KafkaConnection;
  groupId: string;
  /** อ่านตั้งแต่ต้น topic ถ้า group นี้ยังไม่เคย commit (default: false) */
  fromBeginning?: boolean;
  /** อ่านหลาย partition พร้อมกัน (default: 1 — ลำดับภายใน partition ยังรักษาไว้เสมอ) */
  concurrency?: number;
  /** config เพิ่มเติมของ consumer (option librdkafka ชั้นนอก เช่น "session.timeout.ms") */
  config?: Omit<KafkaJS.ConsumerConstructorConfig, "kafkaJS">;
}

export interface ProducerAppOptions {
  /** การเชื่อมต่อของ producer — แยกจาก consumer */
  kafka: KafkaConnection;
  /**
   * (ไม่บังคับ) ปรับวิธีส่งของ producer default เช่น { kafkaJS: { idempotent: true, acks: -1 } }
   * ไม่ใส่ = ใช้ค่า default ของ lib — c.getProducer() ใช้ได้เสมอเมื่อมี producer.kafka
   */
  config?: ProducerConfig;
}

export interface KafkaAppOptions {
  /** class (app จะ new ให้) หรือ instance (กรณีต้อง inject dependency เอง) */
  controllers: (Class | object)[];
  consumer: ConsumerOptions;
  /** ไม่ใส่ = handler produce ได้เฉพาะ c.setProducer({ kafka: ... }) ที่ระบุการเชื่อมต่อเอง */
  producer?: ProducerAppOptions;
  onError?: (err: unknown, c: Ctx) => void | Promise<void>;
  /** รอ connect (consumer / producer) นานสุดเท่าไร ก่อนถือว่าล้มเหลว (default: 15000ms) */
  connectTimeoutMs?: number;
}

export function toClient(conn: KafkaConnection): KafkaClient {
  return conn instanceof Confluent.KafkaJS.Kafka ? conn : new Confluent.KafkaJS.Kafka(conn);
}

const defaultOnError = (err: unknown, c: Ctx) => {
  const msg = err instanceof Error ? err.message : String(err);
  console.error(`[kp] ${c.topic}[${c.partition}]@${c.offset} handler error: ${msg} — ข้าม message นี้`);
};

export class KafkaApp {
  readonly #opts: KafkaAppOptions;
  readonly #routes = new Map<string, { name: string; handler: Handler }[]>();
  #consumer?: KafkaConsumer;
  readonly #producers: ProducerManager;

  constructor(opts: KafkaAppOptions) {
    this.#opts = opts;
    const p = opts.producer;
    this.#producers = new ProducerManager(p ? toClient(p.kafka) : undefined, p?.config, opts.connectTimeoutMs);
    for (const c of opts.controllers) {
      const instance = typeof c === "function" ? new (c as Class)() : c;
      const cls = instance.constructor;
      if (!isController(cls)) throw new TypeError(`${cls.name} ไม่มี @Controller()`);

      const handlers = collectHandlers(instance);
      if (handlers.length === 0) console.warn(`[kp] ${cls.name} ไม่มี method ที่ใส่ @Consumer()`);
      for (const { topic, name, handler } of handlers) {
        const list = this.#routes.get(topic) ?? [];
        list.push({ name: `${cls.name}.${name}`, handler });
        this.#routes.set(topic, list);
      }
    }
  }

  /** topic → handler ที่ลงทะเบียนไว้ (ไว้ debug) */
  get routes(): Record<string, string[]> {
    return Object.fromEntries([...this.#routes].map(([t, hs]) => [t, hs.map((h) => h.name)]));
  }

  async start(): Promise<void> {
    if (this.#consumer) throw new Error("KafkaApp เริ่มทำงานไปแล้ว");
    const { kafka, groupId, fromBeginning = false, concurrency = 1, config } = this.#opts.consumer;
    const onError = this.#opts.onError ?? defaultOnError;

    // producer connect ก่อน → handler แรกก็ส่งได้ทันที และถ้า config ผิดจะ fail ตั้งแต่ start
    await this.#producers.start();

    const consumer = toClient(kafka).consumer({ ...config, kafkaJS: { groupId, fromBeginning } });
    const timeoutMs = this.#opts.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS;
    try {
      await connectWithTimeout(consumer, `consumer (group ${groupId})`, timeoutMs);
    } catch (err) {
      await this.#producers.stop(); // producer connect ไปแล้ว → ปิดด้วย
      throw err;
    }
    this.#consumer = consumer;
    await consumer.subscribe({ topics: [...this.#routes.keys()] });

    await consumer.run({
      partitionsConsumedConcurrently: concurrency,
      eachMessage: async ({ topic, partition, message }) => {
        const c = new Ctx(topic, partition, message, this.#producers);
        // handler หลายตัวบน topic เดียวกัน ทำงานตามลำดับที่ลงทะเบียน
        for (const { handler } of this.#routes.get(topic) ?? []) {
          try {
            await handler(c);
          } catch (err) {
            await onError(err, c); // throw ต่อได้ → message จะถูกส่งมาใหม่
          }
        }
      },
    });
  }

  /** ปิด consumer ก่อน (handler ที่ค้างอยู่จะได้ส่ง message จนเสร็จ) แล้วค่อยปิด producer */
  async stop(): Promise<void> {
    await this.#consumer?.disconnect();
    this.#consumer = undefined;
    await this.#producers.stop();
  }
}
