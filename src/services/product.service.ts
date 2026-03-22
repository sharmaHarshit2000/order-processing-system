/**
 * Product Service - Cache-aside pattern with real PostgreSQL
 *
 * Flow:
 *  GET product → check Redis cache → hit  → return (fast, ~1ms)
 *                                 → miss → query PostgreSQL → cache 5min → return
 */
import type { Product } from "../models/product.model.js";
import { findProductById, findAllProducts } from "../db/repositories/product.repository.js";
import { getCache, setCache } from "../redis/cache.repository.js";

const PRODUCT_CACHE_TTL = 300; // 5 minutes

export async function getProduct(id: string): Promise<Product | null> {
  const cacheKey = `product:${id}`;

  const cached = await getCache<Product>(cacheKey);
  if (cached) {
    console.log(`[Product Service] Cache HIT: ${id}`);
    return cached;
  }

  console.log(`[Product Service] Cache MISS: ${id} — querying PostgreSQL`);
  const product = await findProductById(id);

  if (product) {
    await setCache(cacheKey, product, PRODUCT_CACHE_TTL);
  }

  return product;
}

export async function getAllProducts(): Promise<Product[]> {
  return findAllProducts();
}
