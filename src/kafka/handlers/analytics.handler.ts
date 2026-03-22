/**
 * Analytics Event Handler (Kafka Consumer)
 *
 * Listens to: order.events, analytics.events topics
 * Group: analytics-service-group
 *
 * Stores analytics data in Redis (counters + sorted sets)
 */
import type { EachMessagePayload } from "kafkajs";
import redisClient from "../../redis/client.js";
import type { KafkaEvent, OrderEvent } from "../topics.js";

export async function handleAnalyticsEvent(payload: EachMessagePayload): Promise<void> {
  const raw = payload.message.value?.toString();
  if (!raw) return;

  const event: KafkaEvent<OrderEvent> = JSON.parse(raw);
  const { eventType, payload: data } = event;

  const today = new Date().toISOString().split("T")[0]; // YYYY-MM-DD

  if (eventType === "order.created") {
    // Increment daily order count
    await redisClient.incr(`analytics:orders:${today}`);

    // Increment revenue counter (store in cents to avoid float issues)
    const revenueInCents = Math.round(data.totalAmount * 100);
    await redisClient.incrBy(`analytics:revenue:${today}`, revenueInCents);

    // Track orders per user (sorted set: user → order count)
    await redisClient.zIncrBy("analytics:top_users", 1, data.userId);

    console.log(`[Analytics] Order created tracked for user ${data.userId}`);
  }

  if (eventType === "order.paid") {
    await redisClient.incr(`analytics:paid_orders:${today}`);
  }

  if (eventType === "order.cancelled") {
    await redisClient.incr(`analytics:cancelled_orders:${today}`);
  }
}

export async function getDailyStats(date: string): Promise<{
  orders: number;
  revenue: number;
  paidOrders: number;
  cancelledOrders: number;
}> {
  const [orders, revenueRaw, paidOrders, cancelledOrders] = await Promise.all([
    redisClient.get(`analytics:orders:${date}`),
    redisClient.get(`analytics:revenue:${date}`),
    redisClient.get(`analytics:paid_orders:${date}`),
    redisClient.get(`analytics:cancelled_orders:${date}`),
  ]);

  return {
    orders: parseInt(orders || "0"),
    revenue: parseInt(revenueRaw || "0") / 100, // Convert cents back to dollars
    paidOrders: parseInt(paidOrders || "0"),
    cancelledOrders: parseInt(cancelledOrders || "0"),
  };
}

export async function getTopUsers(limit = 5): Promise<Array<{ userId: string; orders: number }>> {
  const results = await redisClient.zRangeWithScores("analytics:top_users", 0, limit - 1, { REV: true });
  return results.map((r) => ({ userId: r.value, orders: r.score }));
}
