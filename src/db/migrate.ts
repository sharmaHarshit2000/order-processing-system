/**
 * Migration Runner
 *
 * Reads SQL files from src/db/migrations/ in order.
 * Uses schema_migrations table to track which have run.
 * Safe to call on every startup — idempotent.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import pool from "./client.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export async function runMigrations(): Promise<void> {
  const client = await pool.connect();
  try {
    // Create tracking table if it doesn't exist
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        filename   TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    const migrationsDir = path.join(__dirname, "migrations");
    const files = fs
      .readdirSync(migrationsDir)
      .filter((f) => f.endsWith(".sql"))
      .sort(); // lexicographic order: 001, 002, ...

    for (const file of files) {
      const { rowCount } = await client.query(
        "SELECT 1 FROM schema_migrations WHERE filename = $1",
        [file]
      );

      if (rowCount && rowCount > 0) {
        console.log(`[Migrations] Already applied: ${file}`);
        continue;
      }

      const sql = fs.readFileSync(path.join(migrationsDir, file), "utf8");

      await client.query("BEGIN");
      await client.query(sql);
      await client.query("INSERT INTO schema_migrations (filename) VALUES ($1)", [file]);
      await client.query("COMMIT");

      console.log(`[Migrations] Applied: ${file}`);
    }

    console.log("[Migrations] All migrations up to date");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}
