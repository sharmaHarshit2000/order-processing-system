/**
 * Kafka Topic Definitions
 *
 * Event-driven architecture:
 *  - order.events    → order lifecycle (created, updated, cancelled)
 *  - inventory.events → stock changes
 *  - analytics.events → user activity for reporting
 *  - notification.events → trigger emails/SMS/push
 */

export const TOPICS = {
  ORDER_EVENTS: "order.events",
  INVENTORY_EVENTS: "inventory.events",
  ANALYTICS_EVENTS: "analytics.events",
  NOTIFICATION_EVENTS: "notification.events",
} as const;

export type OrderEventType =
  | "order.created"
  | "order.confirmed"
  | "order.paid"
  | "order.shipped"
  | "order.delivered"
  | "order.cancelled"
  | "order.failed";

export type InventoryEventType =
  | "inventory.reserved"
  | "inventory.released"
  | "inventory.low_stock";

export type AnalyticsEventType =
  | "product.viewed"
  | "order.completed"
  | "user.registered";

export interface KafkaEvent<T = unknown> {
  eventType: string;
  timestamp: string;
  payload: T;
}

export interface OrderEvent {
  orderId: string;
  userId: string;
  status: string;
  totalAmount: number;
  items: Array<{ productId: string; quantity: number }>;
}

export interface InventoryEvent {
  productId: string;
  quantityChange: number;
  newStock: number;
  reason: string;
}
