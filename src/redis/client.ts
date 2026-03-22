import { createClient } from "redis";
import { config } from "../config/env.js";

const redisClient = createClient({
  socket: {
    host: config.redis.host,
    port: config.redis.port,
  },
  password: config.redis.password,
});

redisClient.on("connect", () => console.log("[Redis] Connecting..."));
redisClient.on("ready", () => console.log("[Redis] Ready"));
redisClient.on("error", (err) => console.error("[Redis] Error:", err.message));
redisClient.on("reconnecting", () => console.log("[Redis] Reconnecting..."));

export async function connectRedis() {
  await redisClient.connect();
}

export async function disconnectRedis() {
  await redisClient.disconnect();
}

export default redisClient;
