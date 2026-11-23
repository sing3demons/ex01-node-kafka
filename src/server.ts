import { EventConsumerController } from "./controller.js";
import { onShutdown, kafka } from "./kafka.js";
import { KafkaApp } from "./kp/app.js";

const app = new KafkaApp({
    kafka,
    groupId: process.argv[2] ?? "kp-billing",
    fromBeginning: true,
    controllers: [EventConsumerController],
    // ไม่ใส่ onError = log แล้วข้าม message ที่ error
    // onError: (err, c) => { throw err },   // ← แบบนี้ message จะถูกส่งมาใหม่ (retry)
});

console.log("routes:", app.routes);
await app.start();
onShutdown(() => app.stop());
