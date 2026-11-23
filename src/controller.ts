/**
 * บทที่ 4: เขียน consumer แบบ Controller ด้วย lib KP (src/kp)
 *
 * แทนที่จะเขียน consumer.run({ eachMessage }) เอง แล้ว if/else ตาม topic
 * เราประกาศ handler เป็น method แล้วให้ KafkaApp route message ตาม topic ให้
 *
 *   @Controller()         → class นี้คือกลุ่ม handler
 *   @Consumer("orders")   → method นี้รับ message จาก topic "orders"
 *   c: Ctx                → headers / payload / validate ของ message นั้น
 *
 * รัน: npm run controller            (group "kp-billing")
 *      npm run producer              (อีก terminal — BatchDemo จะ validate ไม่ผ่าน ดู error log)
 */
import { z } from "zod";
import { kafka, onShutdown, TOPIC } from "./kafka.js";
import { Consumer, Controller, Ctx, formatIssues, KafkaApp } from "./kp/index.js";

const OrderCreatedSchema = z.object({
  orderId: z.string(),
  customerId: z.string(),
  amount: z.number().nonnegative(),
  createdAt: z.iso.datetime(),
});

type EventHeaders = { "event-type"?: string };

@Controller()
export class EventConsumerController {
  @Consumer(TOPIC)
  async handleEvents(c: Ctx) {
    const headers = c.headers<EventHeaders>();

    // payload<T>() แค่บอก type — ยังไม่ได้ตรวจจริง
    const raw = c.payload<{ orderId?: string }>();
    console.log(`→ ${c.topic}[${c.partition}]@${c.offset} key=${c.key} type=${headers["event-type"]} id=${raw.orderId ?? "-"}`);

    // validate() ตรวจจริงด้วย schema — ไม่ส่ง data จะตรวจ c.payload() ให้
    const result = c.validate(OrderCreatedSchema, c.payload());
    if (!result.ok) {
      throw new Error(`Invalid request: ${formatIssues(result.issues)}`);
    }

    result.value; // type = { orderId: string; customerId: string; amount: number; createdAt: string }
    console.log(`  ✓ order ${result.value.orderId} ของ ${result.value.customerId} ${result.value.amount} บาท`);
  }
}
