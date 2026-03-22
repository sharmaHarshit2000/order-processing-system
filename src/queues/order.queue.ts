/**
 * Order Processing Queue using BullMQ
 *
 * Handles the async lifecycle of an order:
 *  Step 1: payment-processing (charge the card)
 *  Step 2: fulfillment       (pick, pack, ship)
 *
 * Uses job chaining: fulfillment starts only after payment succeeds.
 */
import { Queue } from "bullmq";
import { config } from "../config/env.js";

export interface OrderJobData {
  orderId: string;
  userId: string;
  totalAmount: number;
  items: Array<{ productId: string; quantity: number; unitPrice: number }>;
}

const connection = {
  host: config.redis.host,
  port: config.redis.port,
  password: config.redis.password,
};

export const orderQueue = new Queue<OrderJobData>("order-processing", {
  connection,
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: "exponential", delay: 3000 },
    removeOnComplete: { count: 200 },
    removeOnFail: { count: 100 },
  },
});

export async function addOrderProcessingJob(data: OrderJobData): Promise<void> {
  // Add payment job first
  await orderQueue.add("payment-processing", data, {
    priority: 1, // High priority
  });

  console.log(`[Order Queue] Payment job queued for order: ${data.orderId}`);
}
