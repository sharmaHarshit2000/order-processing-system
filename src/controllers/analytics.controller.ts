import type { Request, Response } from "express";
import { getDailyStats, getTopUsers } from "../kafka/handlers/analytics.handler.js";

export async function getDailyStatsHandler(req: Request, res: Response) {
  const date = (req.query.date as string) || new Date().toISOString().split("T")[0];
  const stats = await getDailyStats(date);
  res.json({ date, stats });
}

export async function getTopUsersHandler(_req: Request, res: Response) {
  const topUsers = await getTopUsers(5);
  res.json({ topUsers });
}
