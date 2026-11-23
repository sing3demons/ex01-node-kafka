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

export const ORDER_EVENTS_TOPIC = "order-events";
export const ORDER_AUDIT_TOPIC = "order-audit";

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
      return;
    }
    console.log(
      `${where} ✓ order ${out.order.id} ของ ${out.order.customerId} ${out.order.amount} บาท (ยอดรวม ${out.customerTotal} บาท)`,
    );

    // แบบที่ 1: producer default (ตั้ง config ไว้ใน server.ts, connect ตั้งแต่ start)
    // ส่ง event ต่อให้ service อื่น — key เดิม → ลำดับของลูกค้าคนเดียวกันยังเรียงถูก
    const [sent] = await c.getProducer().send({
      topic: ORDER_EVENTS_TOPIC,
      messages: [
        {
          key: out.order.customerId,
          value: { orderId: out.order.id, customerId: out.order.customerId, customerTotal: out.customerTotal },
          headers: { "event-type": "OrderAccepted", "caused-by": where },
        },
      ],
    });
    console.log(`  → ${ORDER_EVENTS_TOPIC}[${sent.partition}]@${sent.baseOffset ?? sent.offset} OrderAccepted`);

    // แบบที่ 2: producer ตาม config (connect ตอนใช้ครั้งแรก แล้ว cache ไว้ใช้ต่อ)
    // audit log ไม่ซีเรียสมาก → acks: 1 (รอแค่ leader) เร็วกว่า
    await c.setProducer({ producer: { kafkaJS: { acks: 1 } } }).send({
      topic: ORDER_AUDIT_TOPIC,
      messages: [{ value: { action: "order.created", orderId: out.order.id, at: new Date().toISOString() } }],
    });
  }
}
