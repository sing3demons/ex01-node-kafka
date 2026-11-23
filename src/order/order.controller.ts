/**
 * Controller = ประตูรับ message จาก Kafka
 * หน้าที่มีแค่: อ่าน/validate ข้อมูลจาก Ctx → เรียก service → log ผล
 * ไม่มี business logic และไม่แตะ repository ตรงๆ
 *
 *   Kafka ─► Controller (validate) ─► Service (logic) ─► Repository (DB)
 */
import { TOPIC } from "../kafka.js";
import { Consumer, Controller, Ctx, formatIssues } from "../kp/index.js";
import type { OrderService } from "./order.service.js";
import { OrderCreatedSchema } from "./order.schema.js";

type EventHeaders = { "event-type"?: string };

@Controller()
export class EventConsumerController {
  // รับ service ผ่าน constructor (dependency injection) — server.ts เป็นคนสร้างแล้วส่งเข้ามา
  constructor(private readonly orderService: OrderService) {}

  @Consumer(TOPIC)
  async handleEvents(c: Ctx) {
    const headers = c.headers<EventHeaders>();

    // ไม่ใช่ event ที่เราสนใจ → return เฉยๆ = ถือว่าจัดการแล้ว (commit offset ไปต่อ)
    if (headers["event-type"] !== "OrderCreated") return;

    const result = c.validate(OrderCreatedSchema, c.payload());
    if (!result.ok) {
      throw new Error(`Invalid request: ${formatIssues(result.issues)}`);
    }

    const out = await this.orderService.createOrder(result.value, {
      topic: c.topic,
      partition: c.partition,
      offset: c.offset,
    });

    const where = `${c.topic}[${c.partition}]@${c.offset}`;
    if (out.status === "duplicate") {
      console.log(`${where} ↺ order ${out.orderId} ซ้ำ — ข้าม`);
    } else {
      console.log(
        `${where} ✓ order ${out.order.id} ของ ${out.order.customerId} ${out.order.amount} บาท (ยอดรวม ${out.customerTotal} บาท)`,
      );
    }
  }
}
