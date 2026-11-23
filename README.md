# เรียน Kafka ด้วย TypeScript

ตัวอย่าง order service ที่อ่าน event จาก Kafka ด้วย lib **KP** (`src/kp`) ซึ่งเป็น lib เล็กๆ ที่ครอบ
[`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript) ให้เขียน consumer แบบ `@Controller` / `@Consumer`
และแบ่งโค้ดเป็น controller → service → repository

## เนื้อหา

- [เริ่มต้น](#เริ่มต้น)
- [ส่ง message ทดสอบ](#ส่ง-message-ทดสอบ)
- [พื้นฐาน Kafka](#พื้นฐาน-kafka)
- [โครงสร้างโปรเจกต์](#โครงสร้างโปรเจกต์)
- [lib KP](#lib-kp)
  - [Controller และ Ctx](#controller-และ-ctx)
  - [KafkaApp](#kafkaapp)
  - [Produce จากใน handler](#produce-จากใน-handler)
  - [`kafka` กับ `config` ต่างกันอย่างไร](#kafka-กับ-config-ต่างกันอย่างไร)
  - [เมื่อ handler error](#เมื่อ-handler-error)
- [แบบฝึกหัด](#แบบฝึกหัด)
- [kafkajs กับ @confluentinc/kafka-javascript](#kafkajs-กับ-confluentinckafka-javascript)
- [หมายเหตุ](#หมายเหตุ)

## เริ่มต้น

```bash
cp .env.example .env     # username/password ของ Kafka (docker compose กับแอปใช้ไฟล์เดียวกัน)
docker compose up -d     # Kafka (localhost:9092) + Kafka UI (http://localhost:8081)
npm install
npm run start:dev        # รัน order service (consumer group "kp-billing")
npm run typecheck        # ตรวจ type
```

Kafka เปิด SASL/PLAIN ไว้ ทุก client ต้อง login ด้วย `KAFKA_USERNAME` / `KAFKA_PASSWORD`

ถ้ายังไม่มี topic `orders` ให้สร้างแบบ 3 partition (ถ้าปล่อยให้สร้างอัตโนมัติจะได้แค่ 1 partition):

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --command-config /tmp/client.properties --create --if-not-exists --topic orders --partitions 3
```

> `/tmp/client.properties` คือไฟล์ login ที่ healthcheck ของ container สร้างไว้ให้ จึงใช้ได้กับทุกคำสั่งใน container

## ส่ง message ทดสอบ

ใช้ console producer ที่มากับ Kafka แต่ละบรรทัดมีรูปแบบ `header:value<TAB>key<TAB>value`:

```bash
printf '%s\t%s\t%s\n' \
  'event-type:OrderCreated' dave '{"orderId":"o-1","customerId":"dave","amount":250,"createdAt":"2026-11-23T10:00:00Z"}' \
  'event-type:OrderCreated' dave '{"orderId":"o-2","customerId":"dave","amount":"oops","createdAt":"2026-11-23T10:00:00Z"}' \
| docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --producer.config /tmp/client.properties --topic orders \
    --property parse.key=true --property parse.headers=true
```

ผลที่ `npm run start:dev`:

```
orders[1]@0 ✓ order o-1 ของ dave 250 บาท (ยอดรวม 250 บาท)
  → order-events[0]@65 OrderAccepted
[kp] orders[1]@1 handler error: Invalid request: amount: Invalid input: expected number, received string — ข้าม message นี้
```

หรือส่งผ่าน Kafka UI ที่ http://localhost:8081 → Topics → `orders` → **Produce Message**

## พื้นฐาน Kafka

```
 Producer ──send──►  topic "orders"            ◄──poll── Consumer (group: kp-billing)
  key=alice          ├── partition 0  [0][1][2]...
  key=bob            ├── partition 1  [0][1]...
  key=carol          └── partition 2  [0][1][2]...
```

| คำ | ความหมาย |
|---|---|
| **topic** | หมวดของ message เช่น `orders` (คล้ายชื่อตาราง) |
| **partition** | topic แบ่งเป็นหลายช่อง ลำดับถูกรักษาไว้ **ภายใน** partition — ยิ่งมาก consumer ยิ่งช่วยกันอ่านได้มาก |
| **offset** | เลขลำดับของ message ใน partition (0, 1, 2, ...) |
| **key** | key เดียวกัน → partition เดียวกันเสมอ → ลำดับของ key นั้นถูกรักษาไว้ |
| **consumer group** | consumer ที่ `groupId` เดียวกัน **แบ่ง** partition กันอ่าน, คนละ group **ต่างคนต่างได้ครบ** |
| **offset commit** | Kafka จำว่า group อ่านถึงไหน → ปิดแล้วเปิดใหม่ อ่านต่อจากเดิม |
| **rebalance** | consumer เข้า/ออกจาก group → Kafka แจก partition ใหม่อัตโนมัติ |

- Producer เขียนต่อท้าย partition (append-only log) และ message **ไม่ถูกลบหลังอ่าน**
- Kafka ส่งแบบ **at-least-once**: message เดิมอาจมาซ้ำได้ (เช่น ตอน rebalance หรือ retry) — โค้ดจึงต้องรับมือกับการซ้ำ

## โครงสร้างโปรเจกต์

```
src/
├── server.ts                 composition root — สร้างและประกอบทุกอย่างเข้าด้วยกัน
├── kafka.ts                  การเชื่อมต่อของ consumer / producer (แยกกัน)
├── order/
│   ├── order.controller.ts   รับ message จาก Kafka: validate → เรียก service → produce event ต่อ
│   ├── order.service.ts      business logic (ไม่รู้จัก Kafka / DB)
│   ├── order.repository.ts   interface + InMemoryOrderRepository
│   └── order.schema.ts       zod schema + type ที่ทุก layer ใช้ร่วมกัน
└── kp/                       lib KP
    ├── app.ts                KafkaApp — route message ตาม topic ไปหา handler
    ├── context.ts            Ctx — headers / payload / validate / producer
    ├── decorators.ts         @Controller / @Consumer
    ├── producer.ts           producer default + on-demand
    ├── connect.ts            connect แบบมี timeout
    ├── standard-schema.ts    interface กลางของ zod / valibot / arktype
    └── index.ts
```

### Controller → Service → Repository

```
Kafka ─► Controller ─────────► Service ─────────► Repository ─► DB
         อ่าน Ctx / validate     business logic      เก็บ/อ่านข้อมูล
         produce event ต่อ       idempotent          (ไม่มี logic)
```

| layer | ทำ | ไม่ทำ |
|---|---|---|
| **Controller** | อ่าน headers/payload, validate, เรียก service, produce event ต่อ | business logic, แตะ DB ตรงๆ |
| **Service** | กฎทางธุรกิจ เช่น กัน order ซ้ำ, คำนวณยอดรวม | รู้จัก Kafka (`Ctx`) หรือรู้ว่า DB คืออะไร |
| **Repository** | อ่าน/เขียนข้อมูล | business logic |

แต่ละ layer รับ dependency ผ่าน constructor แล้ว `server.ts` เป็นคนประกอบ (ไม่ต้องใช้ DI framework):

```ts
const orderRepository = new InMemoryOrderRepository();
const orderService = new OrderService(orderRepository);

const app = new KafkaApp({
  controllers: [new EventConsumerController(orderService)], // ส่ง instance เพราะต้อง inject service
  consumer: { kafka: consumerConnection, groupId: "kp-billing", fromBeginning: true },
  producer: { kafka: producerConnection, config: { kafkaJS: { idempotent: true } } },
});
await app.start();
```

ข้อดี:
- **เปลี่ยน DB ง่าย** — เขียน class ใหม่ที่ `implements OrderRepository` (เช่น `PostgresOrderRepository`) แล้วแก้บรรทัดเดียวใน `server.ts`
- **test ง่าย** — service ไม่พึ่ง Kafka จึงเรียกตรงๆ ได้เลย:

```ts
const svc = new OrderService(new InMemoryOrderRepository());
await svc.createOrder(event, source); // { status: "created", ... }
await svc.createOrder(event, source); // { status: "duplicate", ... }  ← orderId เดิมไม่ถูกบันทึกซ้ำ
```

> `InMemoryOrderRepository` เก็บใน memory — ข้อมูลหายเมื่อปิดโปรแกรม และกันซ้ำได้แค่ภายในการรันครั้งเดียว ใช้ DB จริงเพื่อให้กันซ้ำข้ามการรีสตาร์ตได้

## lib KP

### Controller และ Ctx

```ts
@Controller()
export class EventConsumerController {
  constructor(private readonly orderService: OrderService) {}

  @Consumer("orders")
  async handleEvents(c: Ctx) {
    const headers = c.headers<{ "event-type"?: string }>();
    if (headers["event-type"] !== "OrderCreated") return; // ไม่สนใจ → return = จัดการแล้ว

    const result = c.validate(OrderCreatedSchema, c.payload());
    if (!result.ok) {
      throw new Error(`Invalid request: ${formatIssues(result.issues)}`);
    }
    await this.orderService.createOrder(result.value, { topic: c.topic, partition: c.partition, offset: c.offset });
  }
}
```

| API | หน้าที่ |
|---|---|
| `@Controller()` | ทำเครื่องหมาย class เป็นกลุ่ม handler |
| `@Consumer("topic")` | ผูก method กับ topic — ใส่ซ้อนได้ (method เดียวฟังหลาย topic) และหลาย handler ฟัง topic เดียวกันได้ |
| `c.topic` `c.partition` `c.offset` `c.key` `c.timestamp` | ตำแหน่งของ message |
| `c.headers<T>()` / `c.header(name)` | headers ที่แปลงเป็น string แล้ว |
| `c.payload<T>()` | value ที่ parse JSON แล้ว (parse ครั้งเดียว; ไม่ใช่ JSON ได้ string) — `<T>` เป็นแค่ type hint **ไม่ได้ตรวจจริง** |
| `c.validate(Schema, data?)` | ตรวจด้วย [Standard Schema](https://standardschema.dev) (zod, valibot, arktype) → `{ ok: true, value }` หรือ `{ ok: false, issues }` — ไม่ส่ง `data` = ตรวจ payload |
| `c.validateAsync(...)` | สำหรับ schema ที่มี async refine |
| `formatIssues(issues)` | แปลง issues เป็นข้อความ เช่น `amount: Invalid input: expected number` |
| `c.getProducer()` | producer default — ดู [Produce จากใน handler](#produce-จากใน-handler) |
| `c.setProducer({ kafka?, producer? })` | producer ตาม config ที่ส่งเข้าไป |

decorator ใช้ได้ทั้ง TC39 decorators (default ของ TypeScript) และ `experimentalDecorators: true` แบบ NestJS

### KafkaApp

consumer กับ producer **เชื่อมต่อแยกกัน** — คนละ client, clientId, user หรือแม้แต่ broker ได้

| option | ความหมาย |
|---|---|
| `controllers` | class (app จะ `new` ให้) หรือ instance ที่ inject dependency แล้ว |
| `consumer.kafka` | การเชื่อมต่อของ consumer: `Kafka` instance หรือ `{ kafkaJS: { brokers, sasl, clientId } }` |
| `consumer.groupId` | ชื่อ consumer group |
| `consumer.fromBeginning` | อ่านตั้งแต่ต้น topic ถ้า group นี้ยังไม่เคย commit (default `false`) |
| `consumer.concurrency` | จำนวน partition ที่ทำพร้อมกัน (default `1`) — ลำดับภายใน partition ยังถูกรักษาไว้ |
| `consumer.config` | option ของ librdkafka เพิ่มเติม เช่น `"session.timeout.ms"` |
| `producer.kafka` | การเชื่อมต่อของ producer — ใส่แค่นี้ก็ใช้ `c.getProducer()` ได้ (connect ตอน `start()`) |
| `producer.config` | (ไม่บังคับ) วิธีส่งของ producer default — ดู[ด้านล่าง](#kafka-กับ-config-ต่างกันอย่างไร) |
| `onError` | จัดการเมื่อ handler throw — ดู [เมื่อ handler error](#เมื่อ-handler-error) |
| `connectTimeoutMs` | รอ connect นานสุด (default `15000`) — ถ้า login ผิด librdkafka จะ retry ไม่จบ จึงตัดแล้วให้ `start()` throw |

| method | ความหมาย |
|---|---|
| `app.start()` | connect producer → connect consumer → subscribe ทุก topic ที่มี `@Consumer` → เริ่มอ่าน |
| `app.stop()` | ปิด consumer ก่อน (handler ที่ค้างจะได้ส่งจนเสร็จ) แล้วค่อยปิด producer ทุกตัว |
| `app.routes` | topic → handler ที่ลงทะเบียนไว้ (ไว้ debug) |

**การเชื่อมต่อใน [src/kafka.ts](src/kafka.ts):** แต่ละฝั่งอ่าน env ของตัวเองก่อน ถ้าไม่มีจึงใช้ค่ากลาง

| ฝั่ง | clientId | env (ลำดับที่อ่าน) |
|---|---|---|
| consumer | `order-service-consumer` | `KAFKA_CONSUMER_USERNAME` → `KAFKA_USERNAME` (เช่นเดียวกับ `PASSWORD`, `BROKERS`) |
| producer | `order-service-producer` | `KAFKA_PRODUCER_USERNAME` → `KAFKA_USERNAME` |

ดูตัวอย่างใน [.env.example](.env.example)

### Produce จากใน handler

มี producer 2 แบบ ทั้งคู่เป็น client **แยกจาก consumer**:

| | `c.getProducer()` | `c.setProducer({...})` |
|---|---|---|
| ตั้งค่าที่ | `new KafkaApp({ producer: { kafka, config } })` | ตอนเรียกใช้ |
| connect เมื่อ | `app.start()` (ก่อน consumer เริ่ม — config ผิดรู้ทันที) | `send()` ครั้งแรก |
| ใช้ซ้ำ | ตัวเดียวทั้ง app | cache ตาม config — config เดิมได้ตัวเดิม ไม่ connect ใหม่ทุก message |
| เหมาะกับ | event หลักของ service | งานที่ต้องการ config ต่างจากปกติ หรือส่งไป cluster อื่น |

```ts
// แบบที่ 1: producer default
await c.getProducer().send({
  topic: "order-events",
  messages: [{ key: "alice", value: { orderId: "o1" }, headers: { "event-type": "OrderAccepted" } }],
}); // value ที่เป็น object จะถูก JSON.stringify ให้

// แบบที่ 2: ไม่ใส่ kafka = ใช้การเชื่อมต่อของ producer.kafka แต่ config ต่างไป
await c.setProducer({ producer: { kafkaJS: { acks: 1 } } }).send({ topic: "order-audit", messages: [...] });

// แบบที่ 2: ใส่ kafka = ต่อ cluster / user อื่น
await c.setProducer({ kafka: { kafkaJS: { brokers: ["other:9092"], sasl } } }).send(...);
```

- หลัง `c.setProducer(...)` แล้ว `c.getProducer()` ใน message เดียวกันจะคืนตัวที่ set ไว้
- ถ้าไม่ได้ใส่ `producer` ใน `KafkaApp` เลย ต้องส่ง `kafka` เองใน `c.setProducer()` ทุกครั้ง
- topic ที่ยังไม่มีจะถูกสร้างอัตโนมัติตอน produce ครั้งแรก แต่ได้แค่ **1 partition**

### `kafka` กับ `config` ต่างกันอย่างไร

```ts
producer: {
  kafka: producerConnection,                    // ต่อ "ที่ไหน ด้วย user อะไร" (brokers, sasl, clientId) — บังคับ
  config: { kafkaJS: { idempotent: true } },    // ส่ง "แบบไหน" — ไม่บังคับ (ไม่ใส่ = ค่า default ของ lib)
},
```

ตัวเลือกที่ใส่ใน `config` ได้:

```ts
config: {
  kafkaJS: {
    idempotent: true,                      // กัน message ซ้ำเมื่อ producer retry
    acks: -1,                              // 0 = ไม่รอ, 1 = รอ leader, -1 = รอทุก replica
    compression: CompressionTypes.GZIP,    // บีบอัด → ประหยัด network / disk
  },
  "linger.ms": 5,                          // option ของ librdkafka ใส่ชั้นนอก: รอรวม message 5ms เป็น batch
},
```

#### `idempotent: true` กันอะไร

กัน message **ซ้ำเพราะ producer ส่งใหม่ (retry)**:

```
ไม่มี idempotent:
  producer ──send(A)──► broker   เขียน A แล้ว ✓
  producer ◄─── ✗ ───── broker   แต่ตอบกลับไม่ถึง (network สะดุด / timeout)
  producer ──send(A)──► broker   producer คิดว่าไม่สำเร็จ ส่งใหม่ → A ถูกเขียน 2 ครั้ง ❌

มี idempotent:
  send(A, seq=5) → เขียน ✓ (ตอบกลับหาย)
  send(A, seq=5) → broker: "seq 5 ของ producer นี้เขียนไปแล้ว" → ไม่เขียนซ้ำ ✓
```

broker ให้ **Producer ID** กับ producer แต่ละตัว และ message ได้ **sequence number** ต่อ partition — broker จึงรู้ว่าอันไหนเคยเขียนแล้ว
นอกจากนี้ยังรับประกันว่า **ลำดับไม่สลับ** ตอน retry และบังคับ `acks: -1` (ปลอดภัยสุด แลกกับช้ากว่า `acks: 1` เล็กน้อย)

#### สิ่งที่ idempotent กัน**ไม่ได้**

กันซ้ำได้แค่ภายใน producer ตัวเดียวใน session เดียว กรณีนี้ยังซ้ำได้:

```
consumer อ่าน order X → ส่ง OrderAccepted ✓ → แอปพังก่อน commit offset
เปิดแอปใหม่ → อ่าน order X อีกรอบ → ส่ง OrderAccepted อีกครั้ง ❌ (เป็นการ send ใหม่ ไม่ใช่ retry)
```

ต้องกันที่ระดับ app — เช่น ฝั่งผู้รับเช็ค `orderId` ซ้ำแบบที่ `OrderService` ทำ — หรือใช้ Kafka transactions (exactly-once) ซึ่งซับซ้อนกว่า

ในตัวอย่าง: `OrderAccepted` ใช้ `getProducer()` ที่เปิด idempotent เพราะเป็น event สำคัญ
ส่วน audit log ใช้ `setProducer({ producer: { kafkaJS: { acks: 1 } } })` ยอมเสี่ยงหายได้นิดหน่อยเพื่อความเร็ว

### เมื่อ handler error

handler throw → KafkaApp เรียก `onError(err, c)`:

| `onError` | ผล |
|---|---|
| ไม่ใส่ (default) | log แล้ว **ข้าม** — commit offset ไปต่อ |
| คืนค่าปกติ | ถือว่าจัดการแล้ว commit ไปต่อ |
| throw ต่อ เช่น `(err) => { throw err }` | **ไม่ commit** → Kafka ส่ง message นี้มาใหม่ (retry แบบ at-least-once) |

ระวัง: ถ้าเลือก retry แล้ว message ผิดถาวร (เช่น validate ไม่ผ่าน) จะวนไม่จบ — ปกติควร retry เฉพาะ error ชั่วคราว (DB ล่ม, timeout)

## แบบฝึกหัด

เปิด `npm run start:dev` ไว้ แล้วส่ง message ด้วยคำสั่งใน [ส่ง message ทดสอบ](#ส่ง-message-ทดสอบ)

1. **key → partition** — ส่ง key เดียวกันหลายครั้ง แล้วสังเกตว่า `orders[?]` เป็น partition เดิมเสมอ ลองเปลี่ยน key ดู
2. **validate** — ส่ง `amount` เป็น string หรือลบ field `orderId` → ดู error จาก `formatIssues`
3. **filter ด้วย header** — ส่ง `event-type:Other` → handler `return` ทันที ไม่มี log
4. **idempotent ระดับ service** — ส่ง `orderId` เดิมซ้ำ → ได้ `↺ order ... ซ้ำ — ข้าม`
5. **offset ถูกจำไว้** — ปิดแอป (Ctrl+C) ส่ง message เพิ่ม แล้วเปิดใหม่ → อ่านต่อเฉพาะอันใหม่ ไม่เริ่มจากต้น
6. **consumer group แบ่งงาน** — เปิด `npm run start:dev` 2 terminal → แต่ละตัวได้ partition คนละชุด (ดูที่ Kafka UI → Consumers → `kp-billing`) ลองปิดตัวหนึ่ง → อีกตัวได้ partition ทั้งหมด (rebalance)
7. **retry** — เปิด `onError: (err) => { throw err }` ใน `server.ts` แล้วส่ง message ที่ validate ไม่ผ่าน → message วนกลับมาซ้ำ (ข้อเสียของการ retry error ถาวร)
8. **การเชื่อมต่อแยกกัน** — รัน `KAFKA_PRODUCER_PASSWORD=wrong npm run start:dev` → `start()` fail ที่ producer ภายใน 15 วินาที ส่วน consumer ไม่ได้เริ่ม
9. **ดูผลลัพธ์** — Kafka UI → Topics → `order-events` / `order-audit` → ดู message ที่ controller produce ออกไป

## kafkajs กับ @confluentinc/kafka-javascript

branch นี้ใช้ `@confluentinc/kafka-javascript` (Confluent ดูแล ข้างในเป็น librdkafka ตัวเดียวกับ client Python/Go/.NET) ผ่าน API แบบ KafkaJS — ส่วน branch `kafkajs` เป็นบทเรียนพื้นฐานที่ใช้ `kafkajs`

| เรื่อง | kafkajs | @confluentinc/kafka-javascript |
|---|---|---|
| สร้าง client | `new Kafka({ brokers, sasl })` | `new Kafka({ kafkaJS: { brokers, sasl } })` |
| acks / compression | ใส่ตอน `send()` | ใส่ตอนสร้าง `producer({ kafkaJS: { acks, compression } })` |
| fromBeginning | ใส่ตอน `subscribe()` | ใส่ตอนสร้าง `consumer({ kafkaJS: { fromBeginning } })` |
| subscribe | `{ topic }` | `{ topics: [...] }` |
| ดู partition ที่ได้ | `consumer.on(GROUP_JOIN)` | `rebalance_cb` (option ของ librdkafka) |
| partitioner | `createPartitioner` | ค่า default `murmur2_random` (เหมือน Java) |
| option ระดับล่าง | — | ใส่ชั้นนอกได้เลย เช่น `"linger.ms": 5` |

## หมายเหตุ

- `@confluentinc/kafka-javascript` เป็น native module (C++) — `npm install` จะโหลด binary ที่ build ไว้แล้วสำหรับ OS/Node ของเรา
- สลับ branch แล้ว `npm run typecheck` error ว่าหา type `node` ไม่เจอ → รัน `npm install` ใหม่
- ตอนกด Ctrl+C แอปอาจใช้เวลา 2–3 วินาที เพราะ consumer ต้องแจ้งออกจาก group ก่อน
- ตอน login ผิด จะเห็น log `Authentication failed` ของ librdkafka ซ้ำๆ จนครบ `connectTimeoutMs` — เป็นเรื่องปกติ
- docker-compose ไม่มี volume — ถ้า container `kafka` ถูกสร้างใหม่ (เช่น แก้ env ใน compose) topic และ message จะหายหมด
