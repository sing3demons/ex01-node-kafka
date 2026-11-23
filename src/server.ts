/**
 * server.ts = composition root — ที่เดียวที่ "ประกอบ" dependency ทั้งหมดเข้าด้วยกัน
 *
 *   repository → service → controller → KafkaApp
 *
 * อยากเปลี่ยน DB: เปลี่ยนบรรทัด repository บรรทัดเดียว (เช่น new PostgresOrderRepository(pool))
 */
import { consumerConnection, onShutdown, producerConnection } from "./kafka.js";
import { KafkaApp } from "./kp/index.js";
import { EventConsumerController } from "./order/order.controller.js";
import { InMemoryOrderRepository } from "./order/order.repository.js";
import { OrderService } from "./order/order.service.js";

const orderRepository = new InMemoryOrderRepository();
const orderService = new OrderService(orderRepository);

const app = new KafkaApp({
  // ส่ง instance ที่ inject dependency แล้ว (ไม่ใช่ class) เพราะ controller ต้องการ service
  controllers: [new EventConsumerController(orderService)],

  // consumer กับ producer เชื่อมต่อแยกกัน (คนละ client, ตั้ง user/broker ต่างกันได้)
  consumer: {
    kafka: consumerConnection,
    groupId: "kp-billing",
    fromBeginning: true,
  },
  producer: {
    kafka: producerConnection,
    // (ไม่บังคับ) ปรับวิธีส่ง — idempotent กัน message ซ้ำเมื่อ producer retry
    config: { kafkaJS: { idempotent: true } },
  },

  // ไม่ใส่ onError = log แล้วข้าม message ที่ error
  // onError: (err, c) => { throw err },   // ← แบบนี้ message จะถูกส่งมาใหม่ (retry)
});

console.log("routes:", app.routes);
await app.start();
onShutdown(() => app.stop());
