/**
 * connect แบบมี timeout
 *
 * librdkafka จะ retry ไปเรื่อยๆ เมื่อ login ไม่ผ่านหรือต่อ broker ไม่ได้ — connect() จึงค้างไม่จบ
 * ครอบด้วย timeout แล้ว disconnect ทิ้ง เพื่อให้ app.start() fail พร้อม error ที่อ่านรู้เรื่อง
 */
export const DEFAULT_CONNECT_TIMEOUT_MS = 15_000;

export async function connectWithTimeout(
  client: { connect(): Promise<void>; disconnect(): Promise<void> },
  label: string,
  timeoutMs: number,
): Promise<void> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`[kp] ${label} connect ไม่สำเร็จภายใน ${timeoutMs}ms — ตรวจ brokers / username / password`)),
      timeoutMs,
    );
  });
  try {
    await Promise.race([client.connect(), timeout]);
  } catch (err) {
    await client.disconnect().catch(() => {}); // หยุด librdkafka ไม่ให้ retry ค้างอยู่เบื้องหลัง
    throw err;
  } finally {
    clearTimeout(timer);
  }
}
