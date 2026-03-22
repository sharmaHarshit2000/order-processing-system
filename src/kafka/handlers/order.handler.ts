/**
 * Order Event Handler (Kafka Consumer — inventory-service-group)
 *
 * Listens to: order.events
 *
 * On order.created:
 *  - Stock already deducted in order service (synchronously, before Kafka publish)
 *  - Here: invalidate Redis cache + update leaderboard + check low stock
 *
 * On order.cancelled:
 *  - Return stock to PostgreSQL using a transaction
 *  - Invalidate Redis cache
 */
import type { EachMessagePayload } from "kafkajs";
import pool from "../../db/client.js";
import { findProductById, returnStockTransactional } from "../../db/repositories/product.repository.js";
import { deleteCache } from "../../redis/cache.repository.js";
import { incrementProductSales } from "../../redis/leaderboard.repository.js";
import { publishEvent } from "../producer.js";
import { TOPICS } from "../topics.js";
import type { KafkaEvent, OrderEvent, InventoryEvent } from "../topics.js";

const LOW_STOCK_THRESHOLD = 5;

export async function handleOrderEventForInventory(payload: EachMessagePayload): Promise<void> {
  const raw = payload.message.value?.toString();
  if (!raw) return;

  const event: KafkaEvent<OrderEvent> = JSON.parse(raw);
  const { eventType, payload: orderData } = event;

  if (eventType === "order.created") {
    for (const item of orderData.items) {
      // Invalidate cache so next read fetches fresh stock from PostgreSQL
      await deleteCache(`product:${item.productId}`);

      // Update sales leaderboard in Redis sorted set
      await incrementProductSales(item.productId, item.quantity);

      // Check if stock is now low — read from DB (cache was just invalidated)
      const product = await findProductById(item.productId);
      if (product && product.stock <= LOW_STOCK_THRESHOLD) {
        await publishEvent<InventoryEvent>(
          TOPICS.INVENTORY_EVENTS,
          {
            eventType: "inventory.low_stock",
            timestamp: new Date().toISOString(),
            payload: {
              productId: item.productId,
              quantityChange: -item.quantity,
              newStock: product.stock,
              reason: "order_fulfilled",
            },
          },
          item.productId
        );
        console.warn(`[Inventory Handler] LOW STOCK ALERT: ${item.productId} — ${product.stock} remaining`);
      }
    }
  }

  if (eventType === "order.cancelled") {
    // Return stock to PostgreSQL inside a transaction
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await returnStockTransactional(orderData.items, client);
      await client.query("COMMIT");
      console.log(`[Inventory Handler] Stock returned for cancelled order ${orderData.orderId}`);
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

    // Invalidate affected product caches
    for (const item of orderData.items) {
      await deleteCache(`product:${item.productId}`);
    }
  }
}
