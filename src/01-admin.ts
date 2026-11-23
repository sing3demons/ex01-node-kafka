/**
 * บทที่ 1: Admin — Topic และ Partition
 *
 * Topic      = "หมวด" ของ message เช่น orders, payments (คล้ายชื่อตาราง)
 * Partition  = topic ถูกแบ่งเป็นหลายช่อง (log) message ในแต่ละ partition เรียงลำดับเสมอ
 *              ยิ่งมีหลาย partition → consumer ในกลุ่มเดียวกันช่วยกันอ่านได้หลายตัว (scale)
 * Offset     = เลขลำดับของ message ในแต่ละ partition (0, 1, 2, ...)
 *
 *   topic "orders"
 *   ├── partition 0: [0][1][2][3] ...
 *   ├── partition 1: [0][1] ...
 *   └── partition 2: [0][1][2] ...
 *
 * รัน: npm run admin
 */
import { kafka, TOPIC } from "./kafka.js";

const admin = kafka.admin();
await admin.connect();

// สร้าง topic 3 partition (ถ้ามีอยู่แล้วจะได้ false กลับมา ไม่ error)
const created = await admin.createTopics({
  topics: [{ topic: TOPIC, numPartitions: 3, replicationFactor: 1 }],
});
console.log(created ? `สร้าง topic "${TOPIC}" แล้ว` : `topic "${TOPIC}" มีอยู่แล้ว`);

console.log("\nTopic ทั้งหมดใน cluster:", await admin.listTopics());

// ดูว่าแต่ละ partition มี message ถึง offset ไหนแล้ว
// low = offset แรกที่ยังเก็บอยู่, high = offset ที่ message ถัดไปจะได้
const offsets = await admin.fetchTopicOffsets(TOPIC);
console.log(`\nOffset ของ "${TOPIC}":`);
console.table(
  offsets.map((o) => ({
    partition: o.partition,
    low: o.low,
    high: o.high,
    "จำนวน message": Number(o.high) - Number(o.low),
  })),
);

await admin.disconnect();
