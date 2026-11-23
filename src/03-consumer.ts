/**
 * บทที่ 3: Consumer และ Consumer Group
 *
 * Consumer Group (groupId):
 *   - consumer ที่ groupId เดียวกัน "แบ่งกัน" อ่าน — 1 partition ถูกอ่านโดย consumer 1 ตัวในกลุ่ม
 *   - consumer ต่าง groupId "ต่างคนต่างอ่าน" ได้ message ครบทุกอันทั้งคู่
 *
 *   topic orders (3 partitions)         group "billing"
 *   ├── p0 ──────────────────────────►  consumer A
 *   ├── p1 ──────────────────────────►  consumer A
 *   └── p2 ──────────────────────────►  consumer B
 *
 *   ถ้ามี consumer มากกว่า partition → ตัวที่เกินจะว่าง (ไม่ได้ partition)
 *
 * Offset commit:
 *   Kafka จำไว้ว่า group นี้อ่านถึง offset ไหนแล้ว (เก็บใน topic __consumer_offsets)
 *   ปิดแล้วเปิดใหม่ → อ่านต่อจากเดิม ไม่เริ่มใหม่
 *   fromBeginning มีผลแค่ตอน group "ยังไม่เคย" commit อะไรเลย
 *
 * Rebalance:
 *   เมื่อ consumer เข้า/ออกจากกลุ่ม Kafka จะแจก partition ใหม่ให้อัตโนมัติ
 *
 * รัน: npm run consumer                    (group "billing")
 *      npm run consumer -- billing A       (ตั้งชื่อ consumer ให้ดูง่าย)
 *      npm run consumer -- analytics       (อีก group → ได้ message ครบเหมือนกัน)
 */
import { ErrorCodes, kafka, onShutdown, TOPIC } from "./kafka.js";

const groupId = process.argv[2] ?? "billing";
const name = process.argv[3] ?? `consumer-${process.pid}`;
const tag = `[${groupId}/${name}]`;

const consumer = kafka.consumer({
  kafkaJS: {
    groupId,
    // fromBeginning ของ lib นี้ตั้งที่ config (kafkajs ตั้งตอน subscribe)
    fromBeginning: true,
  },

  // rebalance_cb = option ของ librdkafka: ถูกเรียกทุกครั้งที่ Kafka แจก partition ใหม่
  rebalance_cb: (err: { code: number }, assignment: { topic: string; partition: number }[]) => {
    const partitions = assignment.map((a) => a.partition).join(", ");
    if (err.code === ErrorCodes.ERR__ASSIGN_PARTITIONS) {
      console.log(`${tag} rebalance → ได้ partition [${partitions}]`);
    } else if (err.code === ErrorCodes.ERR__REVOKE_PARTITIONS) {
      console.log(`${tag} rebalance → คืน partition [${partitions}]`);
    }
  },
});

await consumer.connect();
await consumer.subscribe({ topics: [TOPIC] });

await consumer.run({
  // autoCommit (default true) = lib commit offset ให้อัตโนมัติหลัง eachMessage ทำงานสำเร็จ
  // ถ้า eachMessage throw error → ไม่ commit → message นั้นจะถูกอ่านใหม่ (at-least-once)
  eachMessage: async ({ partition, message }) => {
    const key = message.key?.toString();
    const eventType = message.headers?.["event-type"]?.toString();
    const value = JSON.parse(message.value?.toString() ?? "{}");

    console.log(
      `${tag} p${partition} offset ${message.offset.padStart(3)} | key=${key} | ${eventType} |`,
      value.orderId ?? value.type,
      value.amount !== undefined ? `${value.amount} บาท` : "",
    );

    // จำลองว่าการประมวลผลใช้เวลา (ลองเปิด consumer หลายตัวจะเห็นว่าช่วยกันอ่าน)
    await new Promise((r) => setTimeout(r, 200));
  },
});

console.log(`${tag} รอ message อยู่... (Ctrl+C เพื่อออก)`);
onShutdown(() => consumer.disconnect());
