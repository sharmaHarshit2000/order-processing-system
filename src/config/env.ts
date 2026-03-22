import "dotenv/config";

export const config = {
  redis: {
    host: process.env.REDIS_HOST || "localhost",
    port: parseInt(process.env.REDIS_PORT || "6379"),
    password: process.env.REDIS_PASSWORD || undefined,
  },
  kafka: {
    brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
    clientId: process.env.KAFKA_CLIENT_ID || "order-processing-system",
    groupId: process.env.KAFKA_GROUP_ID || "order-group",
  },
  app: {
    port: parseInt(process.env.PORT || "3000"),
    nodeEnv: process.env.NODE_ENV || "development",
  },
  db: {
    host: process.env.DB_HOST || "localhost",
    port: parseInt(process.env.DB_PORT || "5432"),
    name: process.env.DB_NAME || "ops_db",
    user: process.env.DB_USER || "ops_user",
    password: process.env.DB_PASSWORD || "ops_pass",
    poolMax: parseInt(process.env.DB_POOL_MAX || "10"),
  },
};
