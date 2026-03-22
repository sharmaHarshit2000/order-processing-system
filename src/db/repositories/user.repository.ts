import pool from "../client.js";
import type { User } from "../../models/user.model.js";

export async function findUserById(id: string): Promise<User | null> {
  const { rows } = await pool.query<User>(
    "SELECT id, name, email, tier FROM users WHERE id = $1",
    [id]
  );
  return rows[0] ?? null;
}

export async function findAllUsers(): Promise<User[]> {
  const { rows } = await pool.query<User>(
    "SELECT id, name, email, tier FROM users ORDER BY name"
  );
  return rows;
}
