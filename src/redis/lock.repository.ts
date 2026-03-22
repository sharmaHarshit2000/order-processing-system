/**
 * Distributed Lock using Redis SET NX PX
 *
 * Why: Prevent race conditions when multiple instances
 *      try to update the same inventory at the same time.
 *
 * Pattern: Optimistic locking with TTL safety net
 *  - SET key token NX PX ttl  → acquire lock (only if not exists)
 *  - DEL key (if token matches) → release lock
 *  - TTL ensures lock is auto-released if process crashes
 */
import redisClient from "./client.js";
import { randomUUID } from "crypto";

const LOCK_TTL_MS = 5000; // 5 seconds max hold time

export interface Lock {
  token: string;
  resource: string;
}

export async function acquireLock(resource: string): Promise<Lock | null> {
  const key = `lock:${resource}`;
  const token = randomUUID();

  const result = await redisClient.set(key, token, {
    NX: true,       // Only set if key does NOT exist
    PX: LOCK_TTL_MS // Auto-expire after 5s (prevents deadlocks)
  });

  if (result === "OK") {
    console.log(`[Lock] Acquired lock on: ${resource}`);
    return { token, resource };
  }

  console.log(`[Lock] Failed to acquire lock on: ${resource} (already locked)`);
  return null;
}

export async function releaseLock(lock: Lock): Promise<void> {
  const key = `lock:${lock.resource}`;
  const current = await redisClient.get(key);

  // Only release if WE own the lock (token matches)
  // This prevents releasing someone else's lock
  if (current === lock.token) {
    await redisClient.del(key);
    console.log(`[Lock] Released lock on: ${lock.resource}`);
  } else {
    console.warn(`[Lock] Lock already expired or owned by another process: ${lock.resource}`);
  }
}

export async function withLock<T>(
  resource: string,
  fn: () => Promise<T>
): Promise<T> {
  const lock = await acquireLock(resource);
  if (!lock) throw new Error(`Could not acquire lock on resource: ${resource}`);

  try {
    return await fn();
  } finally {
    await releaseLock(lock);
  }
}
