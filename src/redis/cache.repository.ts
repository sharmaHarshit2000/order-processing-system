/**
 * Generic cache repository using Redis.
 * Supports TTL-based expiration and JSON serialization.
 *
 * Pattern: Cache-Aside (Lazy Loading)
 *  1. Check Redis → hit → return cached data
 *  2. Miss → fetch from source → write to Redis → return
 */
import redisClient from "./client.js";

const DEFAULT_TTL = 300; // 5 minutes

export async function getCache<T>(key: string): Promise<T | null> {
  const data = await redisClient.get(key);
  if (!data) return null;
  return JSON.parse(data) as T;
}

export async function setCache<T>(key: string, value: T, ttlSeconds = DEFAULT_TTL): Promise<void> {
  await redisClient.setEx(key, ttlSeconds, JSON.stringify(value));
}

export async function deleteCache(key: string): Promise<void> {
  await redisClient.del(key);
}

export async function deleteCachePattern(pattern: string): Promise<void> {
  // scanIterator yields string[] batches — use SCAN not KEYS (safe for production)
  let count = 0;
  for await (const batch of redisClient.scanIterator({ MATCH: pattern, COUNT: 100 })) {
    for (const key of batch) {
      await redisClient.del(key);
      count++;
    }
  }
  if (count > 0) {
    console.log(`[Cache] Invalidated ${count} keys matching: ${pattern}`);
  }
}
