# เรียน Kafka ด้วย TypeScript

ตัวอย่างนี้สอนพื้นฐาน Kafka ผ่านโค้ดสั้นๆ 3 บท แต่ละไฟล์มีคำอธิบายแนวคิดอยู่ใน comment ด้านบน

| บท | ไฟล์ | เรียนรู้เรื่อง |
|---|---|---|
| 0 | [src/kafka.ts](src/kafka.ts) | การสร้าง client และการ login ด้วย SASL (username/password) |
| 1 | [src/01-admin.ts](src/01-admin.ts) | topic, partition, offset |
| 2 | [src/02-producer.ts](src/02-producer.ts) | การส่ง message, key → partition, headers, acks, batch |
| 3 | [src/03-consumer.ts](src/03-consumer.ts) | consumer group, offset commit, rebalance |

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

ดู topic, message และ consumer group ได้ที่ Kafka UI: http://localhost:8081

## หมายเหตุ

- ตอนเปิด consumer ครั้งแรกอาจเห็น log `The group coordinator is not available` — เป็นแค่ช่วงที่ Kafka เตรียม group ครั้งแรก kafkajs จะ retry ให้เอง
- ตอนกด Ctrl+C consumer อาจใช้เวลา 2-3 วินาที เพราะต้องแจ้งออกจาก group ก่อน
