/**
 * Seed Script — run once to populate initial data
 *
 * Run: npm run seed
 *
 * Uses ON CONFLICT DO NOTHING so it's safe to run multiple times.
 */
import "dotenv/config";
import { connectDB } from "./client.js";
import { runMigrations } from "./migrate.js";
import pool from "./client.js";

async function seed() {
  console.log("[Seed] Starting...");

  await connectDB();
  await runMigrations();

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    await client.query(`
      INSERT INTO users (id, name, email, tier) VALUES
        ('u1', 'Alice Johnson',  'alice@example.com',   'vip'),
        ('u2', 'Bob Smith',      'bob@example.com',     'premium'),
        ('u3', 'Charlie Brown',  'charlie@example.com', 'free')
      ON CONFLICT (id) DO NOTHING
    `);
    console.log("[Seed] Users inserted");

    await client.query(`
      INSERT INTO products (id, name, price, stock, category) VALUES
        ('p1', 'iPhone 15',   999.00,  50,  'electronics'),
        ('p2', 'MacBook Pro', 2499.00, 20,  'electronics'),
        ('p3', 'AirPods Pro', 249.00,  100, 'electronics'),
        ('p4', 'Nike Air Max', 150.00, 200, 'footwear'),
        ('p5', 'Sony PS5',    499.00,  15,  'gaming')
      ON CONFLICT (id) DO NOTHING
    `);
    console.log("[Seed] Products inserted");

    await client.query("COMMIT");
    console.log("[Seed] Done!");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
    await pool.end();
  }
}

seed().catch((err) => {
  console.error("[Seed] Failed:", err);
  process.exit(1);
});
