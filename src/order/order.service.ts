/**
 * Service = business logic — ไม่รู้จัก Kafka (ไม่มี Ctx) และไม่รู้ว่า DB คืออะไร
 * จึงเรียกใช้ซ้ำได้จากทุกทาง (Kafka consumer, HTTP, cron) และ test ได้ง่าย
 */
import type { OrderRepository } from "./order.repository.js";
import type { Order, OrderCreatedEvent } from "./order.schema.js";

export type CreateOrderResult =
  | { status: "created"; order: Order; customerTotal: number }
  | { status: "duplicate"; orderId: string };

export class OrderService {
  constructor(private readonly orders: OrderRepository) {}

  /**
   * Kafka ส่งแบบ at-least-once → message เดิมอาจมาซ้ำ (เช่น ตอน rebalance หรือ retry)
   * service ต้อง "idempotent": ทำซ้ำกี่ครั้งผลก็เหมือนเดิม — ที่นี่ใช้ orderId กันซ้ำ
   */
  async createOrder(event: OrderCreatedEvent, source: Order["source"]): Promise<CreateOrderResult> {
    const order: Order = {
      id: event.orderId,
      customerId: event.customerId,
      amount: event.amount,
      status: "CREATED",
      createdAt: new Date(event.createdAt),
      source,
    };

    const inserted = await this.orders.insert(order);
    if (!inserted) return { status: "duplicate", orderId: event.orderId };

    const customerTotal = await this.orders.sumAmountByCustomer(order.customerId);
    return { status: "created", order, customerTotal };
  }
}
