/**
 * KP — lib เล็กๆ ครอบ @confluentinc/kafka-javascript ให้เขียน consumer แบบ controller + decorator
 *
 *   @Controller()
 *   export class OrderController {
 *     @Consumer("orders")
 *     async onOrder(c: Ctx) {
 *       const result = c.validate(OrderSchema);
 *       if (!result.ok) throw new Error("Invalid request");
 *       result.value; // ได้ type จาก schema
 *     }
 *   }
 */
export {
  KafkaApp,
  type ConsumerOptions,
  type KafkaAppOptions,
  type KafkaConnection,
  type ProducerAppOptions,
} from "./app.js";
export { Ctx, formatIssues, type RawMessage, type ValidateResult } from "./context.js";
export { Consumer, Controller } from "./decorators.js";
export type {
  KpProducer,
  ProduceMessage,
  ProduceRecord,
  ProducerConfig,
  ProducerOptions,
  RecordMetadata,
} from "./producer.js";
export type { StandardSchemaV1 } from "./standard-schema.js";
