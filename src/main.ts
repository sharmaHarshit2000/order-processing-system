/**
 * Main Entry Point — HTTP API server
 *
 * Run: npm run dev
 *
 * Boots: Redis → PostgreSQL → Migrations → Kafka Producer → Express
 *
 * Also run separately:
 *  npm run workers          → BullMQ workers
 *  npm run kafka:consumers  → Kafka consumer groups
 *  npm run seed             → Seed initial data (first time only)
 */
import "dotenv/config";
import app from "./app.js";
import { connectRedis } from "./redis/client.js";
import { connectDB } from "./db/client.js";
import { runMigrations } from "./db/migrate.js";
import { connectProducer } from "./kafka/producer.js";
import { config } from "./config/env.js";

async function bootstrap() {
  console.log("=".repeat(55));
  console.log("  Order Processing System — Starting");
  console.log("=".repeat(55));

  await connectRedis();
  await connectDB();
  await runMigrations();
  await connectProducer();

  app.listen(config.app.port, () => {
    console.log(`\n[Server] Running on http://localhost:${config.app.port}`);
    console.log("\nEndpoints:");
    console.log("  GET    /health");
    console.log("  GET    /api/products");
    console.log("  GET    /api/products/:id");
    console.log("  GET    /api/products/leaderboard");
    console.log("  GET    /api/products/:id/rank");
    console.log("  POST   /api/orders          (header: x-user-id)");
    console.log("  GET    /api/orders/:id      (header: x-user-id)");
    console.log("  DELETE /api/orders/:id      (header: x-user-id)");
    console.log("  GET    /api/analytics/daily");
    console.log("  GET    /api/analytics/top-users");
    console.log("\nAlso run:");
    console.log("  npm run workers          → BullMQ workers");
    console.log("  npm run kafka:consumers  → Kafka consumers");
    console.log("=".repeat(55));
  });
}

bootstrap().catch((err) => {
  console.error("[Fatal] Failed to start:", err);
  process.exit(1);
});
