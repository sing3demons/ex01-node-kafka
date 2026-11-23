/**
 * kafka.ts — สร้าง Kafka client ที่ทุกบทเรียนใช้ร่วมกัน
 *
 * Kafka client 1 ตัว สร้างได้ 3 อย่าง:
 *   - kafka.admin()     → จัดการ topic / ดูข้อมูล cluster
 *   - kafka.producer()  → ส่ง (เขียน) message เข้า topic
 *   - kafka.consumer()  → อ่าน message จาก topic
 */
import { Kafka, logLevel } from "kafkajs";

export const TOPIC = "orders";

export const kafka = new Kafka({
  clientId: "kafka-tutorial",
  // broker = server ของ Kafka (ในที่นี้มีตัวเดียว คือ container "kafka")
  brokers: (process.env.KAFKA_BROKERS ?? "localhost:9092").split(","),

  // docker-compose เปิด SASL/PLAIN ไว้ → ต้องส่ง username/password
  sasl: {
    mechanism: "plain",
    username: process.env.KAFKA_USERNAME ?? "admin",
    password: process.env.KAFKA_PASSWORD ?? "admin-secret",
  },
  ssl: false, // SASL_PLAINTEXT = มี login แต่ไม่เข้ารหัส (พอสำหรับเครื่อง dev)

  logLevel: logLevel.WARN,
});

/** ปิดโปรแกรมให้เรียบร้อยเมื่อกด Ctrl+C (สำคัญกับ consumer: จะได้ออกจาก group ทันที) */
export function onShutdown(fn: () => Promise<void>) {
  for (const signal of ["SIGINT", "SIGTERM"] as const) {
    process.once(signal, async () => {
      console.log(`\n[${signal}] กำลังปิดการเชื่อมต่อ...`);
      await fn();
      process.exit(0);
    });
  }
}
