/**
 * Kafka Consumers Entry Point
 *
 * Run: npm run kafka:consumers
 *
 * 3 independent consumer groups:
 *  1. inventory-service-group   → updates stock, leaderboard, low-stock alerts
 *  2. analytics-service-group   → writes counters to Redis
 *  3. notification-service-group → queues BullMQ email jobs
 */
import "dotenv/config";
import { connectRedis } from "./redis/client.js";
import { connectDB } from "./db/client.js";
import { connectProducer, ensureTopics } from "./kafka/producer.js";
import { createConsumer } from "./kafka/consumer.js";
import { handleOrderEventForInventory } from "./kafka/handlers/order.handler.js";
import { handleAnalyticsEvent } from "./kafka/handlers/analytics.handler.js";
import { handleNotificationEvent } from "./kafka/handlers/notification.handler.js";
import { TOPICS } from "./kafka/topics.js";

async function startConsumers() {
  console.log("=".repeat(55));
  console.log("  Kafka Consumers — Starting");
  console.log("=".repeat(55));

  await connectRedis();
  await connectDB();       // Inventory handler writes stock back to PostgreSQL on cancellation
  await connectProducer(); // Inventory handler publishes low_stock events

  await ensureTopics([TOPICS.ORDER_EVENTS, TOPICS.INVENTORY_EVENTS]);

  await createConsumer(
    "inventory-service-group",
    [TOPICS.ORDER_EVENTS],
    handleOrderEventForInventory
  );

  await createConsumer(
    "analytics-service-group",
    [TOPICS.ORDER_EVENTS],
    handleAnalyticsEvent
  );

  await createConsumer(
    "notification-service-group",
    [TOPICS.ORDER_EVENTS, TOPICS.INVENTORY_EVENTS],
    handleNotificationEvent
  );

  console.log("\n[Consumers] All groups running:");
  console.log("  inventory-service-group    → order.events");
  console.log("  analytics-service-group    → order.events");
  console.log("  notification-service-group → order.events + inventory.events");
}

startConsumers().catch((err) => {
  console.error("[Fatal] Consumers failed to start:", err);
  process.exit(1);
});
