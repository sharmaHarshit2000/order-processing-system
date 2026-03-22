import express from "express";
import router from "./routes/index.js";

const app = express();

app.use(express.json());

// Health check
app.get("/health", (_req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() });
});

app.use("/api", router);

// Global error handler
app.use((err: Error, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  console.error("[App Error]", err.message);
  res.status(500).json({ error: err.message });
});

export default app;
