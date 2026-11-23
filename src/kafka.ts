/**
 * kafka.ts — สร้าง Kafka client ที่ทุกบทเรียนใช้ร่วมกัน
 *
 * ใช้ @confluentinc/kafka-javascript (Confluent ดูแล, ข้างในคือ librdkafka ภาษา C
 * ตัวเดียวกับที่ client Python/Go/.NET ใช้) ผ่าน API แบบ "KafkaJS" ซึ่งหน้าตาคล้าย kafkajs
 *
 * config มี 2 ชั้น:
 *   kafkaJS: { ... }   → option แบบ kafkajs (brokers, sasl, groupId, ...)
 *   ชั้นนอก            → option ของ librdkafka โดยตรง (เช่น "rebalance_cb", "linger.ms")
 *
 * Kafka client สร้างได้ 3 อย่าง:
 *   - kafka.admin()     → จัดการ topic / ดูข้อมูล cluster
 *   - kafka.producer()  → ส่ง (เขียน) message เข้า topic
 *   - kafka.consumer()  → อ่าน message จาก topic
 */
import Confluent from "@confluentinc/kafka-javascript";

export const { Kafka, CompressionTypes, ErrorCodes, logLevel } = Confluent.KafkaJS;

export const TOPIC = "orders";


/**
 * สร้าง config การเชื่อมต่อ — consumer กับ producer ใช้แยกกัน
 * อ่าน env ตาม prefix ก่อน (เช่น KAFKA_CONSUMER_USERNAME) ถ้าไม่มีใช้ค่ากลาง (KAFKA_USERNAME)
 * → แยก user / สิทธิ์ / broker ของฝั่งอ่านกับฝั่งเขียนได้ แต่ dev ใช้ค่าเดียวกันก็ได้
 */
function connection(role: "CONSUMER" | "PRODUCER", clientId: string) {
  const env = (name: string) => process.env[`KAFKA_${role}_${name}`] ?? process.env[`KAFKA_${name}`];
  return {
    kafkaJS: {
      clientId,
      // broker = server ของ Kafka (ในที่นี้มีตัวเดียว คือ container "kafka")
      brokers: (env("BROKERS") ?? "localhost:9092").split(","),

      // docker-compose เปิด SASL/PLAIN ไว้ → ต้องส่ง username/password
      sasl: {
        mechanism: "plain" as const,
        username: env("USERNAME") ?? "admin",
        password: env("PASSWORD") ?? "admin-secret",
      },
      ssl: false, // SASL_PLAINTEXT = มี login แต่ไม่เข้ารหัส (พอสำหรับเครื่อง dev)

      logLevel: logLevel.WARN,
    },
  };
}

export const consumerConnection = connection("CONSUMER", "order-service-consumer");
export const producerConnection = connection("PRODUCER", "order-service-producer");

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
