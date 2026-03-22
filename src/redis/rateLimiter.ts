/**
 * Sliding Window Rate Limiter using Redis
 *
 * Fetches user tier from PostgreSQL, cached in Redis for 5min
 * so we don't hit the DB on every single request.
 */
import redisClient from "./client.js";
import type { Request, Response, NextFunction } from "express";
import { findUserById } from "../db/repositories/user.repository.js";

const RATE_LIMITS: Record<string, number> = {
  vip: 100,
  premium: 30,
  free: 10,
};

const WINDOW_MS = 60 * 1000; // 1 minute

async function getUserTier(userId: string): Promise<string> {
  // Cache user tier in Redis to avoid DB hit on every request
  const cacheKey = `user-tier:${userId}`;
  const cached = await redisClient.get(cacheKey);
  if (cached) return cached;

  const user = await findUserById(userId);
  const tier = user?.tier ?? "free";

  await redisClient.setEx(cacheKey, 300, tier); // Cache for 5 minutes
  return tier;
}

export async function checkRateLimit(userId: string): Promise<{
  allowed: boolean;
  remaining: number;
  limit: number;
}> {
  const tier = await getUserTier(userId);
  const limit = RATE_LIMITS[tier] ?? RATE_LIMITS.free;

  const key = `ratelimit:${userId}`;
  const now = Date.now();
  const windowStart = now - WINDOW_MS;

  // Sliding window: remove timestamps outside the 1-minute window
  await redisClient.zRemRangeByScore(key, 0, windowStart);

  const count = await redisClient.zCard(key);

  if (count >= limit) {
    return { allowed: false, remaining: 0, limit };
  }

  await redisClient.zAdd(key, { score: now, value: `${now}-${Math.random()}` });
  await redisClient.expire(key, 60);

  return { allowed: true, remaining: limit - count - 1, limit };
}

export function rateLimiterMiddleware(req: Request, res: Response, next: NextFunction) {
  const userId = req.headers["x-user-id"] as string;

  if (!userId) {
    res.status(401).json({ error: "Missing x-user-id header" });
    return;
  }

  checkRateLimit(userId)
    .then(({ allowed, remaining, limit }) => {
      res.setHeader("X-RateLimit-Limit", limit);
      res.setHeader("X-RateLimit-Remaining", remaining);

      if (!allowed) {
        res.status(429).json({
          error: "Too many requests",
          message: "Rate limit exceeded. Please slow down.",
          retryAfter: "60 seconds",
        });
        return;
      }

      next();
    })
    .catch(next);
}
