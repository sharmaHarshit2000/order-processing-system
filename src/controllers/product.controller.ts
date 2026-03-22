import type { Request, Response } from "express";
import { getProduct, getAllProducts } from "../services/product.service.js";
import { getTopProducts, getProductRank } from "../redis/leaderboard.repository.js";

export async function listProductsHandler(_req: Request, res: Response) {
  const products = await getAllProducts();
  res.json({ products });
}

export async function getProductHandler(req: Request, res: Response) {
  const product = await getProduct(req.params.id);
  if (!product) {
    res.status(404).json({ error: "Product not found" });
    return;
  }
  res.json({ product });
}

export async function getLeaderboardHandler(_req: Request, res: Response) {
  const topProducts = await getTopProducts(10);
  res.json({ leaderboard: topProducts });
}

export async function getProductRankHandler(req: Request, res: Response) {
  const rank = await getProductRank(req.params.id);
  res.json({ productId: req.params.id, rank: rank ?? "unranked" });
}
