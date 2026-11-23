/**
 * บทที่ 2: Producer — ส่ง message เข้า Kafka
 *
 * message 1 ก้อนประกอบด้วย:
 *   key     → ใช้เลือก partition: key เดียวกัน = ไป partition เดียวกันเสมอ
 *             (จึงรับประกันลำดับของ "ลูกค้าคนเดียวกัน" ได้)
 *             ถ้าไม่ใส่ key จะกระจายไปหลาย partition
 *   value   → ข้อมูลจริง (Kafka มองเป็น bytes — เราส่ง JSON string)
 *   headers → metadata เพิ่มเติม เช่น event type, trace id
 *
 * acks (ผู้ส่งรอการยืนยันแค่ไหน):
 *   0  = ไม่รอเลย (เร็วสุด อาจหาย)
 *   1  = รอ leader เขียนเสร็จ
 *  -1  = รอทุก replica ที่ in-sync เขียนเสร็จ (ปลอดภัยสุด — ค่า default)
 *
 * รัน: npm run producer            (ส่ง 10 message)
 *      npm run producer -- 50      (ส่ง 50 message)
 */
import { CompressionTypes, Partitioners } from "kafkajs";
import { kafka, TOPIC } from "./kafka.js";

type OrderCreated = {
  orderId: string;
  customerId: string;
  amount: number;
  createdAt: string;
};

const count = Number(process.argv[2] ?? 10);
const customers = ["alice", "bob", "carol"];

const producer = kafka.producer({
  // idempotent = Kafka กันไม่ให้ message ซ้ำ ถ้า producer retry ส่งซ้ำ
  idempotent: true,
  // partitioner = วิธีเลือก partition จาก key (hash ของ key % จำนวน partition)
  // DefaultPartitioner ใช้สูตรเดียวกับ Java client → app ภาษาอื่นได้ partition ตรงกัน
  createPartitioner: Partitioners.DefaultPartitioner,
});
await producer.connect();

for (let i = 1; i <= count; i++) {
  const order: OrderCreated = {
    orderId: `order-${Date.now()}-${i}`,
    customerId: customers[i % customers.length],
    amount: Math.round(Math.random() * 1000),
    createdAt: new Date().toISOString(),
  };

  const [result] = await producer.send({
    topic: TOPIC,
    acks: -1,
    compression: CompressionTypes.GZIP,
    messages: [
      {
        key: order.customerId, // ลูกค้าคนเดียวกัน → partition เดียวกัน
        value: JSON.stringify(order),
        headers: { "event-type": "OrderCreated" },
      },
    ],
  });

  // Kafka ตอบกลับว่า message ไปอยู่ partition ไหน ที่ offset เท่าไร
  console.log(
    `ส่งแล้ว key=${order.customerId.padEnd(5)} → partition ${result.partition}, offset ${result.baseOffset}  (${order.orderId}, ${order.amount} บาท)`,
  );
}

// ส่งทีละหลาย message (batch) ใน request เดียว — เร็วกว่าส่งทีละอันมาก
await producer.send({
  topic: TOPIC,
  messages: customers.map((c) => ({
    key: c,
    value: JSON.stringify({ type: "batch-demo", customerId: c }),
    headers: { "event-type": "BatchDemo" },
  })),
});
console.log(`\nส่ง batch อีก ${customers.length} message ใน request เดียว`);
console.log("สังเกต: key เดียวกันจะได้ partition เดียวกันทุกครั้ง");

await producer.disconnect();
