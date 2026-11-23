/**
 * @Controller() และ @Consumer('topic')
 *
 * ใช้ได้ทั้ง TC39 decorators (ค่า default ของ TypeScript 5+) และแบบเก่า
 * (`experimentalDecorators: true` แบบ NestJS) — ข้อมูลถูกแปะไว้บนตัว class / method
 * แล้ว KafkaApp ค่อยอ่านตอน register จึงไม่ต้องพึ่ง reflect-metadata
 */
import type { Ctx } from "./context.js";

const CONTROLLER = Symbol("kp:controller");
const TOPICS = Symbol("kp:topics");

export type Handler = (c: Ctx) => unknown | Promise<unknown>;

type Tagged = { [TOPICS]?: string[]; [CONTROLLER]?: true };

/** ทำเครื่องหมายว่า class นี้เป็น consumer controller */
export function Controller() {
  return (target: Function, _context?: unknown): void => {
    (target as Tagged)[CONTROLLER] = true;
  };
}

/** ผูก method เข้ากับ topic — ใส่ซ้ำหลายอันได้ (method เดียวฟังหลาย topic) */
export function Consumer(topic: string) {
  return (
    target: object,
    contextOrKey?: ClassMethodDecoratorContext | string | symbol,
    descriptor?: PropertyDescriptor,
  ): void => {
    // TC39: (method, context)   |   legacy: (prototype, key, descriptor)
    const isStandard = typeof contextOrKey === "object" && contextOrKey !== null;
    const method: unknown = isStandard ? target : descriptor?.value;
    if (typeof method !== "function") throw new TypeError("@Consumer() ใช้ได้กับ method เท่านั้น");
    ((method as Tagged)[TOPICS] ??= []).push(topic);
  };
}

export function isController(cls: Function): boolean {
  return (cls as Tagged)[CONTROLLER] === true;
}

/** หา method ทั้งหมดที่มี @Consumer — รวม method ที่ inherit มาจาก class แม่ */
export function collectHandlers(instance: object): { topic: string; name: string; handler: Handler }[] {
  const found: { topic: string; name: string; handler: Handler }[] = [];
  const seen = new Set<string>();
  for (let proto = Object.getPrototypeOf(instance); proto && proto !== Object.prototype; proto = Object.getPrototypeOf(proto)) {
    for (const name of Object.getOwnPropertyNames(proto)) {
      if (name === "constructor" || seen.has(name)) continue;
      seen.add(name);
      const fn = Object.getOwnPropertyDescriptor(proto, name)?.value as Tagged | undefined;
      for (const topic of fn?.[TOPICS] ?? []) {
        found.push({ topic, name, handler: (fn as unknown as Handler).bind(instance) });
      }
    }
  }
  return found;
}
