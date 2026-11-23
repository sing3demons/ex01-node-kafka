/**
 * server.ts = composition root — ที่เดียวที่ "ประกอบ" dependency ทั้งหมดเข้าด้วยกัน
 *
 *   repository → service → controller → KafkaApp
 *
 * อยากเปลี่ยน DB: เปลี่ยนบรรทัด repository บรรทัดเดียว (เช่น new PostgresOrderRepository(pool))
 */
import { kafka, onShutdown } from "./kafka.js";
import { KafkaApp } from "./kp/index.js";
import { EventConsumerController } from "./order/order.controller.js";
import { InMemoryOrderRepository } from "./order/order.repository.js";
import { OrderService } from "./order/order.service.js";

const orderRepository = new InMemoryOrderRepository();
const orderService = new OrderService(orderRepository);

const app = new KafkaApp({
  kafka,
  groupId: "kp-billing",
  fromBeginning: true,
  // ส่ง instance ที่ inject dependency แล้ว (ไม่ใช่ class) เพราะ controller ต้องการ service
  controllers: [new EventConsumerController(orderService)],
  // ไม่ใส่ onError = log แล้วข้าม message ที่ error
  // onError: (err, c) => { throw err },   // ← แบบนี้ message จะถูกส่งมาใหม่ (retry)
});

console.log("routes:", app.routes);
await app.start();
onShutdown(() => app.stop());
