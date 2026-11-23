/**
 * Schema + type ของ order — ใช้ร่วมกันทุก layer
 *   OrderCreatedEvent = ข้อมูลที่รับเข้ามาจาก Kafka (validate ที่ controller)
 *   Order             = ข้อมูลที่เราเก็บ (entity ของ repository)
 */
import { z } from "zod";

export const OrderCreatedSchema = z.object({
  orderId: z.string(),
  customerId: z.string(),
  amount: z.number().nonnegative(),
  createdAt: z.iso.datetime(),
});

export type OrderCreatedEvent = z.infer<typeof OrderCreatedSchema>;

export type OrderStatus = "CREATED";

export interface Order {
  id: string;
  customerId: string;
  amount: number;
  status: OrderStatus;
  createdAt: Date;
  /** ตำแหน่งใน Kafka ที่ทำให้เกิด order นี้ — ไว้ตามรอย/debug */
  source: { topic: string; partition: number; offset: string };
}
