/**
 * Order Processing Worker
 *
 * Two job types:
 *  1. "payment-processing" → simulate payment gateway, update DB status, queue fulfillment
 *  2. "fulfillment"        → simulate pick/pack/ship, update DB status, publish Kafka event
 */
import { Worker, Job } from "bullmq";
import { config } from "../../config/env.js";
import type { OrderJobData } from "../order.queue.js";
import { updateOrderStatus } from "../../db/repositories/order.repository.js";
import { publishEvent } from "../../kafka/producer.js";
import { TOPICS } from "../../kafka/topics.js";
import type { OrderEvent } from "../../kafka/topics.js";

const connection = {
  host: config.redis.host,
  port: config.redis.port,
  password: config.redis.password,
};

async function processOrderJob(job: Job<OrderJobData>): Promise<{ success: boolean }> {
  const { orderId, userId, totalAmount, items } = job.data;

  if (job.name === "payment-processing") {
    console.log(`[Order Worker] Processing payment for order ${orderId} — $${totalAmount}`);

    // Simulate payment gateway (1-2s)
    await new Promise((r) => setTimeout(r, 1000 + Math.random() * 1000));

    // Simulate 5% payment failure to demo retry behavior
    if (Math.random() < 0.05) {
      throw new Error(`Payment gateway rejected order ${orderId}`);
    }

    // Persist status to PostgreSQL
    await updateOrderStatus(orderId, "paid");

    // Publish event so Kafka consumers react
    await publishEvent<OrderEvent>(
      TOPICS.ORDER_EVENTS,
      {
        eventType: "order.paid",
        timestamp: new Date().toISOString(),
        payload: { orderId, userId, status: "paid", totalAmount, items },
      },
      orderId
    );

    // Queue fulfillment job (runs after payment succeeds)
    const { orderQueue } = await import("../order.queue.js");
    await orderQueue.add("fulfillment", job.data, { delay: 2000 });

    console.log(`[Order Worker] Payment successful for ${orderId}. Fulfillment queued.`);
    return { success: true };
  }

  if (job.name === "fulfillment") {
    console.log(`[Order Worker] Fulfilling order ${orderId}...`);

    // Simulate pick & pack (2-3s)
    await new Promise((r) => setTimeout(r, 2000 + Math.random() * 1000));

    await updateOrderStatus(orderId, "shipped");

    await publishEvent<OrderEvent>(
      TOPICS.ORDER_EVENTS,
      {
        eventType: "order.shipped",
        timestamp: new Date().toISOString(),
        payload: { orderId, userId, status: "shipped", totalAmount, items },
      },
      orderId
    );

    console.log(`[Order Worker] Order ${orderId} shipped!`);
    return { success: true };
  }

  throw new Error(`Unknown job name: ${job.name}`);
}

export function startOrderWorker(): Worker<OrderJobData> {
  const worker = new Worker<OrderJobData>("order-processing", processOrderJob, {
    connection,
    concurrency: 3,
  });

  worker.on("completed", (job) => {
    console.log(`[Order Worker] "${job.name}" #${job.id} completed`);
  });

  worker.on("failed", (job, err) => {
    console.error(`[Order Worker] "${job?.name}" #${job?.id} failed: ${err.message}`);
  });

  console.log("[Order Worker] Started — concurrency: 3");
  return worker;
}
