import type { PoolClient } from "pg";
import pool from "../client.js";
import type { Order, OrderItem, OrderStatus } from "../../models/order.model.js";

// pg returns snake_case column names
interface OrderRow {
  id: string;
  user_id: string;
  total_amount: string; // NUMERIC comes back as string from pg
  status: string;
  created_at: Date;
  updated_at: Date;
}

interface OrderItemRow {
  order_id: string;
  product_id: string;
  product_name: string;
  quantity: number;
  unit_price: string;
  total_price: string;
}

function rowToOrderItem(row: OrderItemRow): OrderItem {
  return {
    productId: row.product_id,
    productName: row.product_name,
    quantity: row.quantity,
    unitPrice: parseFloat(row.unit_price),
    totalPrice: parseFloat(row.total_price),
  };
}

function rowToOrder(row: OrderRow, items: OrderItem[]): Order {
  return {
    id: row.id,
    userId: row.user_id,
    items,
    totalAmount: parseFloat(row.total_amount),
    status: row.status as OrderStatus,
    createdAt: row.created_at.toISOString(),
    updatedAt: row.updated_at.toISOString(),
  };
}

/**
 * Insert order + all order_items in a single transaction.
 * Caller must pass the same PoolClient used for stock deduction
 * so both operations share the same BEGIN/COMMIT boundary.
 */
export async function insertOrderWithItems(order: Order, client: PoolClient): Promise<void> {
  await client.query(
    `INSERT INTO orders (id, user_id, total_amount, status, created_at, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6)`,
    [order.id, order.userId, order.totalAmount, order.status, order.createdAt, order.updatedAt]
  );

  for (const item of order.items) {
    await client.query(
      `INSERT INTO order_items
         (order_id, product_id, product_name, quantity, unit_price, total_price)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [order.id, item.productId, item.productName, item.quantity, item.unitPrice, item.totalPrice]
    );
  }
}

export async function findOrderById(id: string): Promise<Order | null> {
  const { rows } = await pool.query<OrderRow>(
    "SELECT * FROM orders WHERE id = $1",
    [id]
  );
  if (!rows[0]) return null;

  const { rows: itemRows } = await pool.query<OrderItemRow>(
    "SELECT * FROM order_items WHERE order_id = $1 ORDER BY id",
    [id]
  );

  return rowToOrder(rows[0], itemRows.map(rowToOrderItem));
}

export async function updateOrderStatus(
  id: string,
  status: OrderStatus,
  client?: PoolClient // optional — pass when inside a transaction
): Promise<Order | null> {
  const db = client ?? pool;

  const { rows } = await db.query<OrderRow>(
    "UPDATE orders SET status = $1, updated_at = NOW() WHERE id = $2 RETURNING *",
    [status, id]
  );
  if (!rows[0]) return null;

  const { rows: itemRows } = await pool.query<OrderItemRow>(
    "SELECT * FROM order_items WHERE order_id = $1 ORDER BY id",
    [id]
  );

  return rowToOrder(rows[0], itemRows.map(rowToOrderItem));
}
