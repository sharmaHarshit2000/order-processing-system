import type { Request, Response } from "express";
import { createOrder, getOrder, cancelOrder } from "../services/order.service.js";

export async function createOrderHandler(req: Request, res: Response) {
  const userId = req.headers["x-user-id"] as string;
  const { items } = req.body;

  if (!items || !Array.isArray(items) || items.length === 0) {
    res.status(400).json({ error: "items array is required" });
    return;
  }

  const order = await createOrder({ userId, items });
  res.status(201).json({ success: true, order });
}

export async function getOrderHandler(req: Request, res: Response) {
  const { id } = req.params;
  const order = await getOrder(id);

  if (!order) {
    res.status(404).json({ error: "Order not found" });
    return;
  }

  res.json({ order });
}

export async function cancelOrderHandler(req: Request, res: Response) {
  const userId = req.headers["x-user-id"] as string;
  const { id } = req.params;

  const order = await cancelOrder(id, userId);
  res.json({ success: true, order });
}
