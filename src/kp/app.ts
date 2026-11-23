/**
 * KafkaApp — รวม controller ทั้งหมดเข้า consumer ตัวเดียว แล้ว route message ตาม topic
 *
 *   const app = new KafkaApp({ kafka, groupId: "billing", controllers: [OrderController] });
 *   await app.start();
 *
 * Error: ถ้า handler throw → เรียก onError(err, ctx)
 *   - onError คืนค่าปกติ   → ถือว่าจัดการแล้ว commit offset ไปต่อ (default: log แล้วข้าม)
 *   - onError throw ต่อ     → ไม่ commit, Kafka จะส่ง message นี้มาใหม่ (at-least-once / retry)
 */
import Confluent from "@confluentinc/kafka-javascript";
import { Ctx } from "./context.js";
import { collectHandlers, isController, type Handler } from "./decorators.js";

type KafkaJS = typeof Confluent.KafkaJS;
type KafkaClient = InstanceType<KafkaJS["Kafka"]>;
type KafkaConsumer = ReturnType<KafkaClient["consumer"]>;
type ConsumerConfig = Parameters<KafkaClient["consumer"]>[0];

type Class<T = object> = new () => T;

export interface KafkaAppOptions {
  kafka: KafkaClient;
  groupId: string;
  /** class (app จะ new ให้) หรือ instance (กรณีต้อง inject dependency เอง) */
  controllers: (Class | object)[];
  /** อ่านตั้งแต่ต้น topic ถ้า group นี้ยังไม่เคย commit (default: false) */
  fromBeginning?: boolean;
  /** อ่านหลาย partition พร้อมกัน (default: 1 — ลำดับภายใน partition ยังรักษาไว้เสมอ) */
  concurrency?: number;
  onError?: (err: unknown, c: Ctx) => void | Promise<void>;
  /** config เพิ่มเติมของ consumer (เช่น option librdkafka ชั้นนอก) */
  consumerConfig?: Omit<ConsumerConfig, "kafkaJS">;
}

const defaultOnError = (err: unknown, c: Ctx) => {
  const msg = err instanceof Error ? err.message : String(err);
  console.error(`[kp] ${c.topic}[${c.partition}]@${c.offset} handler error: ${msg} — ข้าม message นี้`);
};

export class KafkaApp {
  readonly #opts: KafkaAppOptions;
  readonly #routes = new Map<string, { name: string; handler: Handler }[]>();
  #consumer?: KafkaConsumer;

  constructor(opts: KafkaAppOptions) {
    this.#opts = opts;
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
    const { kafka, groupId, fromBeginning = false, concurrency = 1, consumerConfig } = this.#opts;
    const onError = this.#opts.onError ?? defaultOnError;

    const consumer = kafka.consumer({ ...consumerConfig, kafkaJS: { groupId, fromBeginning } });
    this.#consumer = consumer;
    await consumer.connect();
    await consumer.subscribe({ topics: [...this.#routes.keys()] });

    await consumer.run({
      partitionsConsumedConcurrently: concurrency,
      eachMessage: async ({ topic, partition, message }) => {
        const c = new Ctx(topic, partition, message);
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

  async stop(): Promise<void> {
    await this.#consumer?.disconnect();
    this.#consumer = undefined;
  }
}
