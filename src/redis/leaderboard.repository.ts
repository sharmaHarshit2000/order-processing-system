/**
 * Product Sales Leaderboard using Redis Sorted Sets
 *
 * Why: Real-time rankings without expensive DB queries.
 *
 * Commands used:
 *  ZINCRBY  - increment score (sales count) atomically
 *  ZREVRANGE - get top N products (highest score first)
 *  ZREVRANK  - get rank of a specific product
 */
import redisClient from "./client.js";

const LEADERBOARD_KEY = "leaderboard:products";

export async function incrementProductSales(productId: string, quantity: number): Promise<void> {
  await redisClient.zIncrBy(LEADERBOARD_KEY, quantity, productId);
  console.log(`[Leaderboard] Product ${productId} sales +${quantity}`);
}

export async function getTopProducts(limit = 10): Promise<Array<{ productId: string; sales: number; rank: number }>> {
  const results = await redisClient.zRangeWithScores(LEADERBOARD_KEY, 0, limit - 1, { REV: true });
  return results.map((item, index) => ({
    productId: item.value,
    sales: item.score,
    rank: index + 1,
  }));
}

export async function getProductRank(productId: string): Promise<number | null> {
  const rank = await redisClient.zRevRank(LEADERBOARD_KEY, productId);
  return rank !== null ? rank + 1 : null; // Convert 0-indexed to 1-indexed
}
