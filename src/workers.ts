/**
 * Workers Entry Point — BullMQ workers
 *
 * Run: npm run workers
 */
import "dotenv/config";
import { connectRedis } from "./redis/client.js";
import { connectDB } from "./db/client.js";
import { connectProducer } from "./kafka/producer.js";
import { startEmailWorker } from "./queues/workers/email.worker.js";
import { startOrderWorker } from "./queues/workers/order.worker.js";

async function startWorkers() {
  console.log("=".repeat(50));
  console.log("  BullMQ Workers — Starting");
  console.log("=".repeat(50));

  await connectRedis();
  await connectDB();       // Order worker reads/writes orders in PostgreSQL
  await connectProducer(); // Order worker publishes Kafka events

  const emailWorker = startEmailWorker();
  const orderWorker = startOrderWorker();

  async function shutdown() {
    console.log("\n[Workers] Shutting down gracefully...");
    await emailWorker.close();
    await orderWorker.close();
    process.exit(0);
  }

  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);

  console.log("\n[Workers] All workers running. Waiting for jobs...");
}

startWorkers().catch((err) => {
  console.error("[Fatal] Workers failed to start:", err);
  process.exit(1);
});
