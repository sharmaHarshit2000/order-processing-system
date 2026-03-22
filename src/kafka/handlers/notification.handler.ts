/**
 * Notification Event Handler (Kafka Consumer — notification-service-group)
 *
 * Listens to: order.events, inventory.events
 * Queues email jobs in BullMQ (non-blocking)
 */
import type { EachMessagePayload } from "kafkajs";
import { addEmailJob } from "../../queues/email.queue.js";
import { findUserById } from "../../db/repositories/user.repository.js";
import type { KafkaEvent, OrderEvent, InventoryEvent } from "../topics.js";

export async function handleNotificationEvent(payload: EachMessagePayload): Promise<void> {
  const raw = payload.message.value?.toString();
  if (!raw) return;

  const event: KafkaEvent<OrderEvent | InventoryEvent> = JSON.parse(raw);
  const { eventType } = event;

  if (eventType === "order.created") {
    const orderData = event.payload as OrderEvent;
    const user = await findUserById(orderData.userId);
    if (!user) return;

    await addEmailJob({
      to: user.email,
      subject: `Order Confirmed — #${orderData.orderId}`,
      template: "order_confirmation",
      data: {
        userName: user.name,
        orderId: orderData.orderId,
        totalAmount: orderData.totalAmount,
        itemCount: orderData.items.length,
      },
    });

    console.log(`[Notification Handler] Confirmation email queued for ${orderData.orderId}`);
  }

  if (eventType === "order.shipped") {
    const orderData = event.payload as OrderEvent;
    const user = await findUserById(orderData.userId);
    if (!user) return;

    await addEmailJob({
      to: user.email,
      subject: `Your order has shipped! — #${orderData.orderId}`,
      template: "order_shipped",
      data: { userName: user.name, orderId: orderData.orderId },
    });
  }

  if (eventType === "inventory.low_stock") {
    const invData = event.payload as InventoryEvent;

    await addEmailJob({
      to: "admin@example.com",
      subject: `LOW STOCK ALERT: Product ${invData.productId}`,
      template: "low_stock_alert",
      data: {
        productId: invData.productId,
        currentStock: invData.newStock,
      },
    });

    console.log(`[Notification Handler] Low stock alert queued for ${invData.productId}`);
  }
}
