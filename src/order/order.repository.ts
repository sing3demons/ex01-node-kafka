/**
 * Repository = ชั้นที่คุยกับที่เก็บข้อมูล (DB) เท่านั้น ไม่มี business logic
 *
 * service พึ่ง "interface" ไม่ใช่ class จริง → เปลี่ยนจาก in-memory เป็น Postgres/Mongo
 * ได้โดยไม่ต้องแก้ service และตอนเขียน test ก็ส่ง fake repository เข้าไปได้
 */
import type { Order } from "./order.schema.js";

export interface OrderRepository {
  findById(id: string): Promise<Order | undefined>;
  /** บันทึก order ใหม่ — คืน false ถ้ามี id นี้อยู่แล้ว (กัน message ซ้ำ) */
  insert(order: Order): Promise<boolean>;
  sumAmountByCustomer(customerId: string): Promise<number>;
}

/** เก็บใน memory — หายเมื่อปิดโปรแกรม เหมาะกับตัวอย่าง/test */
export class InMemoryOrderRepository implements OrderRepository {
  readonly #orders = new Map<string, Order>();

  async findById(id: string) {
    return this.#orders.get(id);
  }

  async insert(order: Order) {
    if (this.#orders.has(order.id)) return false;
    this.#orders.set(order.id, order);
    return true;
  }

  async sumAmountByCustomer(customerId: string) {
    let total = 0;
    for (const o of this.#orders.values()) if (o.customerId === customerId) total += o.amount;
    return total;
  }
}
