/**
 * Product Repository
 *
 * Key function: deductStockTransactional
 *
 * Why both Redis lock AND pg FOR UPDATE?
 *  - Redis lock (withLock):  fast in-memory guard. Prevents the SAME user
 *    from double-submitting two orders that both pass stock check before
 *    either commits. Acquired BEFORE this function is called.
 *
 *  - FOR UPDATE (here): database-level guard. Handles a different scenario:
 *    two DIFFERENT users on two different app instances racing on the same
 *    product. The Redis lock only guards per-user; FOR UPDATE guards the rows.
 *
 *  Both are needed. Neither alone is sufficient in multi-instance deployments.
 */
import type { PoolClient } from "pg";
import pool from "../client.js";
import type { Product } from "../../models/product.model.js";

// pg returns NUMERIC as string — convert to number
function parseProduct(row: Record<string, unknown>): Product {
  return {
    id: row.id as string,
    name: row.name as string,
    price: parseFloat(row.price as string),
    stock: row.stock as number,
    category: row.category as string,
  };
}

export async function findProductById(id: string): Promise<Product | null> {
  const { rows } = await pool.query(
    "SELECT id, name, price, stock, category FROM products WHERE id = $1",
    [id]
  );
  return rows[0] ? parseProduct(rows[0]) : null;
}

export async function findAllProducts(): Promise<Product[]> {
  const { rows } = await pool.query(
    "SELECT id, name, price, stock, category FROM products ORDER BY name"
  );
  return rows.map(parseProduct);
}

/**
 * Atomically check stock and deduct within a pg transaction.
 *
 * IMPORTANT: Call this INSIDE withLock() and INSIDE a BEGIN/COMMIT block.
 *
 * Row locking order: always sorted by product id to prevent deadlocks
 * when two concurrent transactions lock overlapping sets of products.
 */
export async function deductStockTransactional(
  items: Array<{ productId: string; quantity: number }>,
  client: PoolClient
): Promise<Product[]> {
  // Sort ids deterministically to avoid deadlocks
  const sorted = [...items].sort((a, b) => a.productId.localeCompare(b.productId));
  const ids = sorted.map((i) => i.productId);

  // Lock all rows in one query — SELECT ... FOR UPDATE prevents concurrent reads
  const { rows } = await client.query(
    `SELECT id, name, price, stock, category
     FROM products
     WHERE id = ANY($1::text[])
     ORDER BY id
     FOR UPDATE`,
    [ids]
  );

  // Validate stock before deducting anything
  for (const item of sorted) {
    const row = rows.find((r: Record<string, unknown>) => r.id === item.productId);
    if (!row) throw new Error(`Product not found: ${item.productId}`);
    if ((row.stock as number) < item.quantity) {
      throw new Error(
        `Insufficient stock for "${row.name}". Available: ${row.stock}, Requested: ${item.quantity}`
      );
    }
  }

  // All checks passed — deduct stock
  for (const item of sorted) {
    await client.query(
      "UPDATE products SET stock = stock - $1 WHERE id = $2",
      [item.quantity, item.productId]
    );
  }

  return rows.map(parseProduct);
}

/**
 * Return stock (used on order cancellation).
 * Also called inside a transaction.
 */
export async function returnStockTransactional(
  items: Array<{ productId: string; quantity: number }>,
  client: PoolClient
): Promise<void> {
  for (const item of items) {
    await client.query(
      "UPDATE products SET stock = stock + $1 WHERE id = $2",
      [item.quantity, item.productId]
    );
  }
}
