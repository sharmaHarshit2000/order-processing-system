/**
 * Order Service — core business logic with real PostgreSQL + Redis + Kafka + BullMQ
 *
 * Flow for createOrder:
 *  1. Validate user (PostgreSQL)
 *  2. Acquire Redis distributed lock (prevent same-user double submit)
 *  3. BEGIN pg transaction
 *  4. SELECT products FOR UPDATE (prevent cross-user stock races at DB level)
 *  5. Validate stock
 *  6. UPDATE product stock
 *  7. INSERT order + order_items
 *  8. COMMIT transaction
 *  9. Release Redis lock
 * 10. Invalidate Redis product caches
 * 11. Publish "order.created" event to Kafka
 * 12. Queue BullMQ payment job
 *
 * Why Redis lock + pg FOR UPDATE both?
 *  - Redis lock: prevents the SAME user's concurrent HTTP requests from
 *    both passing stock check before either transaction commits.
 *  - FOR UPDATE: prevents DIFFERENT users on different app instances
 *    from racing on the same product rows.
 */
import { randomUUID } from "crypto";
import type { Order, OrderItem } from "../models/order.model.js";
import { findUserById } from "../db/repositories/user.repository.js";
import { deductStockTransactional } from "../db/repositories/product.repository.js";
import {
  insertOrderWithItems,
  findOrderById,
  updateOrderStatus,
} from "../db/repositories/order.repository.js";
import pool from "../db/client.js";
import { withLock } from "../redis/lock.repository.js";
import { deleteCache } from "../redis/cache.repository.js";
import { publishEvent } from "../kafka/producer.js";
import { TOPICS, type OrderEvent } from "../kafka/topics.js";
import { addOrderProcessingJob } from "../queues/order.queue.js";

export interface CreateOrderRequest {
  userId: string;
  items: Array<{ productId: string; quantity: number }>;
}

export async function createOrder(req: CreateOrderRequest): Promise<Order> {
  const user = await findUserById(req.userId);
  if (!user) throw new Error(`User not found: ${req.userId}`);

  // Redis lock per user — fast guard against double-submit
  return withLock(`order-create:${req.userId}`, async () => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      // Deduct stock inside transaction with FOR UPDATE row locking
      const lockedProducts = await deductStockTransactional(req.items, client);

      // Build order items from locked product rows
      const orderItems: OrderItem[] = req.items.map((reqItem) => {
        const product = lockedProducts.find((p) => p.id === reqItem.productId)!;
        const itemTotal = product.price * reqItem.quantity;
        return {
          productId: product.id,
          productName: product.name,
          quantity: reqItem.quantity,
          unitPrice: product.price,
          totalPrice: itemTotal,
        };
      });

      const totalAmount = orderItems.reduce((sum, i) => sum + i.totalPrice, 0);

      const order: Order = {
        id: `ord_${randomUUID().slice(0, 8)}`,
        userId: req.userId,
        items: orderItems,
        totalAmount,
        status: "confirmed",
        createdAt: new Date().toISOString(),
        updatedAt: new Date().toISOString(),
      };

      // Insert order + items in same transaction as stock deduction
      // If order insert fails, stock deduction is also rolled back
      await insertOrderWithItems(order, client);

      await client.query("COMMIT");
      console.log(`[Order Service] Order ${order.id} committed to DB — $${totalAmount}`);

      // Invalidate Redis caches for all affected products
      await Promise.all(req.items.map((i) => deleteCache(`product:${i.productId}`)));

      // Publish to Kafka (triggers inventory handler, analytics, notifications)
      await publishEvent<OrderEvent>(
        TOPICS.ORDER_EVENTS,
        {
          eventType: "order.created",
          timestamp: new Date().toISOString(),
          payload: {
            orderId: order.id,
            userId: req.userId,
            status: "confirmed",
            totalAmount,
            items: req.items,
          },
        },
        order.id // partition key: same order always goes to same partition
      );

      // Queue async payment processing in BullMQ
      await addOrderProcessingJob({
        orderId: order.id,
        userId: req.userId,
        totalAmount,
        items: orderItems.map((i) => ({
          productId: i.productId,
          quantity: i.quantity,
          unitPrice: i.unitPrice,
        })),
      });

      return order;
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release(); // Always return client to pool
    }
  });
}

export async function getOrder(orderId: string): Promise<Order | null> {
  return findOrderById(orderId);
}

export async function cancelOrder(orderId: string, userId: string): Promise<Order> {
  const order = await findOrderById(orderId);
  if (!order) throw new Error(`Order not found: ${orderId}`);
  if (order.userId !== userId) throw new Error("Unauthorized");
  if (!["confirmed", "payment_processing"].includes(order.status)) {
    throw new Error(`Cannot cancel order in status: ${order.status}`);
  }

  const updated = await updateOrderStatus(orderId, "cancelled");

  await publishEvent<OrderEvent>(
    TOPICS.ORDER_EVENTS,
    {
      eventType: "order.cancelled",
      timestamp: new Date().toISOString(),
      payload: {
        orderId,
        userId,
        status: "cancelled",
        totalAmount: order.totalAmount,
        items: order.items.map((i) => ({ productId: i.productId, quantity: i.quantity })),
      },
    },
    orderId
  );

  return updated!;
}
