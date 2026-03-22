import { Router } from "express";
import { rateLimiterMiddleware } from "../redis/rateLimiter.js";
import {
  createOrderHandler,
  getOrderHandler,
  cancelOrderHandler,
} from "../controllers/order.controller.js";
import {
  listProductsHandler,
  getProductHandler,
  getLeaderboardHandler,
  getProductRankHandler,
} from "../controllers/product.controller.js";
import {
  getDailyStatsHandler,
  getTopUsersHandler,
} from "../controllers/analytics.controller.js";

const router = Router();

// Error wrapper for async route handlers
function asyncHandler(fn: Function) {
  return (req: any, res: any, next: any) => {
    Promise.resolve(fn(req, res, next)).catch(next);
  };
}

// Products (no rate limit — public catalog)
router.get("/products", asyncHandler(listProductsHandler));
router.get("/products/leaderboard", asyncHandler(getLeaderboardHandler));
router.get("/products/:id", asyncHandler(getProductHandler));
router.get("/products/:id/rank", asyncHandler(getProductRankHandler));

// Orders (rate limited per user)
router.post("/orders", rateLimiterMiddleware, asyncHandler(createOrderHandler));
router.get("/orders/:id", rateLimiterMiddleware, asyncHandler(getOrderHandler));
router.delete("/orders/:id", rateLimiterMiddleware, asyncHandler(cancelOrderHandler));

// Analytics
router.get("/analytics/daily", asyncHandler(getDailyStatsHandler));
router.get("/analytics/top-users", asyncHandler(getTopUsersHandler));

export default router;
