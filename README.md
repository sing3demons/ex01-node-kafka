# เรียน Kafka ด้วย TypeScript

ตัวอย่างนี้สอนพื้นฐาน Kafka ผ่านโค้ดสั้นๆ 3 บท แต่ละไฟล์มีคำอธิบายแนวคิดอยู่ใน comment ด้านบน

Branch นี้ใช้ [`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript)
(Confluent ดูแล, ข้างในเป็น librdkafka) ผ่าน API แบบ KafkaJS — ส่วน branch `kafkajs` เป็นโค้ดเดียวกันที่ใช้ `kafkajs`

| เรื่อง | kafkajs | @confluentinc/kafka-javascript |
|---|---|---|
| สร้าง client | `new Kafka({ brokers, sasl })` | `new Kafka({ kafkaJS: { brokers, sasl } })` |
| acks / compression | ใส่ตอน `send()` | ใส่ตอนสร้าง `producer({ kafkaJS: { acks, compression } })` |
| fromBeginning | ใส่ตอน `subscribe()` | ใส่ตอนสร้าง `consumer({ kafkaJS: { fromBeginning } })` |
| subscribe | `{ topic }` | `{ topics: [...] }` |
| ดู partition ที่ได้ | `consumer.on(GROUP_JOIN)` | `rebalance_cb` (option ของ librdkafka) |
| partitioner | `createPartitioner` | ค่า default `murmur2_random` (เหมือน Java) |
| option ระดับล่าง | — | ใส่ชั้นนอกได้เลย เช่น `"linger.ms": 5` |

| บท | ไฟล์ | เรียนรู้เรื่อง |
|---|---|---|
| 0 | [src/kafka.ts](src/kafka.ts) | การสร้าง client และการ login ด้วย SASL (username/password) |
| 1 | [src/01-admin.ts](src/01-admin.ts) | topic, partition, offset |
| 2 | [src/02-producer.ts](src/02-producer.ts) | การส่ง message, key → partition, headers, acks, batch |
| 3 | [src/03-consumer.ts](src/03-consumer.ts) | consumer group, offset commit, rebalance |
| 4 | [src/04-controller.ts](src/04-controller.ts) | เขียน consumer แบบ `@Controller` / `@Consumer` ด้วย lib KP ([src/kp](src/kp)) |

## ภาพรวม

```
 Producer ──send──►  topic "orders"            ◄──poll── Consumer (group: billing)
  key=alice          ├── partition 0  [0][1][2]...
  key=bob            ├── partition 1  [0][1]...
  key=carol          └── partition 2  [0][1][2]...
```

- **Producer** เขียน message ต่อท้าย partition (append-only log) — message ไม่ถูกลบหลังอ่าน
- **Consumer** อ่านตาม offset และ commit บอก Kafka ว่าอ่านถึงไหนแล้ว
- **key เดียวกัน → partition เดียวกัน** → ลำดับของ key นั้นถูกรักษาไว้

## เริ่มต้น

```bash
docker compose up -d     # Kafka (localhost:9092) + Kafka UI (http://localhost:8081)
npm install
```

username/password อยู่ใน [.env](.env) (docker compose และแอปอ่านจากไฟล์เดียวกัน)

## แบบฝึกหัด

**1. สร้าง topic**
```bash
npm run admin
```

**2. ส่งและอ่าน message**
```bash
npm run consumer          # terminal 1 — รอ message
npm run producer          # terminal 2 — ส่ง 10 message
```
สังเกตว่า `key=alice` อยู่ partition เดิมทุกครั้ง

**3. Consumer group แบ่งงานกัน** — เปิด 3 terminal:
```bash
npm run consumer -- billing A
npm run consumer -- billing B
npm run producer -- 30
```
A กับ B ได้ partition คนละชุด (เช่น A=[2], B=[0,1]) และไม่มี message ซ้ำกัน
ลองปิด A (Ctrl+C) → B ได้ partition ทั้งหมดแทน (rebalance)

**4. คนละ group = ต่างคนต่างได้ครบ**
```bash
npm run consumer -- analytics
```
group ใหม่จะอ่านตั้งแต่ต้น (`fromBeginning: true`) และได้ message ทุกอัน แม้ billing อ่านไปแล้ว

**5. Offset ถูกจำไว้** — ปิด consumer ของ billing ส่ง message เพิ่ม แล้วเปิดใหม่
→ อ่านต่อเฉพาะอันที่ยังไม่ได้อ่าน ไม่เริ่มใหม่ ลองรัน `npm run admin` ดูจำนวน message ในแต่ละ partition

**6. มากกว่า partition** — เปิด consumer group เดียวกัน 4 ตัว (topic มี 3 partition)
→ ตัวที่ 4 ได้ `partition []` คือว่าง ไม่ได้อ่านอะไร

**7. Controller แบบ decorator (lib KP)**
```bash
npm run controller        # terminal 1
npm run producer          # terminal 2
```
message `OrderCreated` ผ่าน schema ✓ ส่วน `BatchDemo` validate ไม่ผ่าน → handler throw → log แล้วข้าม

ดู topic, message และ consumer group ได้ที่ Kafka UI: http://localhost:8081

## หมายเหตุ

- lib นี้เป็น native module (C++) — `npm install` จะโหลด binary ที่ build ไว้แล้วสำหรับ OS/Node ของเรา
- ข้อความ `รอ message อยู่...` ขึ้นก่อน `rebalance → ได้ partition` เป็นเรื่องปกติ เพราะ `run()` คืนค่าทันที แล้วค่อยเข้ากลุ่มเบื้องหลัง
- ตอนกด Ctrl+C consumer อาจใช้เวลา 2-3 วินาที เพราะต้องแจ้งออกจาก group ก่อน

## lib KP (src/kp)

ครอบ consumer ของ `@confluentinc/kafka-javascript` ให้เขียนเป็น controller:

```ts
import { z } from "zod";
import { Consumer, Controller, Ctx, KafkaApp } from "./kp/index.js";

const OrderSchema = z.object({ orderId: z.string(), amount: z.number() });

@Controller()
export class EventConsumerController {
  @Consumer("orders")
  async handleEvents(c: Ctx) {
    c.headers<{ "event-type"?: string }>(); // headers เป็น string
    c.payload<{ orderId: string }>();       // JSON.parse แล้ว (แค่ type hint)

    const result = c.validate(OrderSchema, c.payload()); // ไม่ส่งตัวที่ 2 = ตรวจ payload
    if (!result.ok) {
      throw new Error("Invalid request");   // result.issues = รายการที่ผิด
    }
    result.value;                            // type มาจาก schema
  }
}

const app = new KafkaApp({ kafka, groupId: "billing", controllers: [EventConsumerController] });
await app.start();
```

| API | หน้าที่ |
|---|---|
| `@Controller()` | ทำเครื่องหมาย class เป็นกลุ่ม handler |
| `@Consumer("topic")` | ผูก method กับ topic (ใส่ซ้อนกันได้ = ฟังหลาย topic, หลาย handler ฟัง topic เดียวกันได้) |
| `c.topic` `c.partition` `c.offset` `c.key` `c.timestamp` | ตำแหน่งของ message |
| `c.headers<T>()` / `c.header(name)` | headers แปลงเป็น string แล้ว |
| `c.payload<T>()` | value ที่ parse JSON แล้ว (parse ครั้งเดียว, ไม่ใช่ JSON ได้ string) |
| `c.validate(Schema, data?)` | ตรวจด้วย [Standard Schema](https://standardschema.dev) — zod, valibot, arktype ใช้ได้หมด → `{ ok: true, value }` หรือ `{ ok: false, issues }` |
| `c.validateAsync(...)` | สำหรับ schema ที่มี async refine |
| `formatIssues(issues)` | แปลง issues เป็นข้อความ |

**KafkaApp options:** `kafka`, `groupId`, `controllers` (class หรือ instance ที่ inject dependency แล้ว),
`fromBeginning`, `concurrency` (จำนวน partition ที่ทำพร้อมกัน), `onError`, `consumerConfig`

**เมื่อ handler throw:** เรียก `onError(err, c)`
- default: log แล้วข้าม (commit offset ไปต่อ)
- ถ้า `onError` throw ต่อ → ไม่ commit → message ถูกส่งมาใหม่ (retry แบบ at-least-once — ระวัง message ที่ผิดถาวรจะวนไม่จบ)

decorator ใช้ได้ทั้ง TC39 decorators (default ของ TypeScript) และ `experimentalDecorators: true` แบบ NestJS
