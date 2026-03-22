import pg from "pg";
import { config } from "../config/env.js";

const { Pool } = pg;

const pool = new Pool({
  host: config.db.host,
  port: config.db.port,
  database: config.db.name,
  user: config.db.user,
  password: config.db.password,
  max: config.db.poolMax,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

pool.on("connect", () => console.log("[DB] New client connected to pool"));
pool.on("error", (err) => console.error("[DB] Pool error:", err.message));

export async function connectDB(): Promise<void> {
  const client = await pool.connect();
  client.release();
  console.log("[DB] PostgreSQL pool ready");
}

export async function disconnectDB(): Promise<void> {
  await pool.end();
  console.log("[DB] PostgreSQL pool closed");
}

export default pool;
