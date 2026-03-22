# Order Processing System

A production-grade backend system built for learning how **PostgreSQL**, **Redis**, **BullMQ**, and **Apache Kafka** work together in a real e-commerce architecture.

---

## What This Project Is

This is a fully functional **order processing backend** for an e-commerce platform. When a customer places an order, the system must:

- Check if the product is in stock
- Prevent two customers from buying the last item at the same time (race condition)
- Save the order to a database
- Send a confirmation email (without slowing down the API response)
- Update a real-time sales leaderboard
- Track analytics (revenue, order counts)
- Alert admins when stock runs low
- Process payment in the background
- Mark the order as shipped after fulfillment

Every one of these things uses a different technology for a specific reason — and this project shows you exactly why.

---

## Tech Stack

| Technology | Version | Why It's Used |
|------------|---------|---------------|
| **Node.js + TypeScript** | ES2022 | Runtime and type safety |
| **Express.js** | v4 | HTTP API server |
| **PostgreSQL** | v16 | Primary database — durable storage for orders, products, users |
| **Redis** | v7 | Cache, rate limiting, distributed locks, leaderboard, analytics |
| **Apache Kafka** | v7.5 | Event streaming — decouples services, fan-out to multiple consumers |
| **BullMQ** | v5 | Job queues — async background processing with retries |
| **pg** | v8 | Official Node.js PostgreSQL client (raw SQL, no ORM) |
| **ioredis** | v5 | Redis client used by BullMQ internally |
| **kafkajs** | v2 | Kafka client for Node.js |
| **tsx** | v4 | Run TypeScript directly without compiling |
| **dotenv** | v17 | Environment variable management |

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        HTTP Client                          │
└─────────────────────────┬───────────────────────────────────┘
                          │ REST API calls
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    Express.js API Server                    │
│                                                             │
│  ┌─────────────────┐   ┌─────────────────────────────────┐  │
│  │ Rate Limiter    │   │         Routes / Controllers    │  │
│  │ (Redis ZSET)    │   │  /products  /orders  /analytics │  │
│  └────────┬────────┘   └───────────────┬─────────────────┘  │
│           │                            │                    │
└───────────┼────────────────────────────┼────────────────────┘
            │                            │
            ▼                            ▼
┌───────────────────────────────────────────────────────────────┐
│                        Order Service                          │
│                                                               │
│  1. Redis LOCK (withLock)  ← prevent same-user double submit  │
│  2. pg BEGIN transaction                                      │
│  3. SELECT products FOR UPDATE ← prevent cross-user race      │
│  4. Validate stock                                            │
│  5. UPDATE products.stock                                     │
│  6. INSERT order + order_items                                │
│  7. pg COMMIT                                                 │
│  8. Invalidate Redis cache                                    │
│  9. Publish to Kafka  ──────────────────────────────────┐    │
│ 10. Add BullMQ job   ─────────────────┐                 │    │
└───────────────────────────────────────┼─────────────────┼────┘
                                        │                 │
               ┌────────────────────────┘                 │
               ▼                                          ▼
┌──────────────────────────┐              ┌───────────────────────────┐
│      BullMQ Queues       │              │      Apache Kafka          │
│                          │              │                           │
│  email-queue             │              │  Topic: order.events      │
│  └─ email.worker         │              │  Topic: inventory.events  │
│     concurrency: 5       │              │                           │
│     retry: 3x expBackoff │              └──────────┬────────────────┘
│                          │                         │
│  order-processing-queue  │              ┌──────────┼────────────────┐
│  └─ order.worker         │              │     3 Consumer Groups      │
│     payment → fulfillment│              │                           │
│     → publishes to Kafka │              │  ┌─────────────────────┐  │
└──────────────────────────┘              │  │ inventory-service   │  │
                                          │  │ deduct/return stock │  │
                                          │  │ update leaderboard  │  │
                                          │  │ low stock alert     │  │
                                          │  └─────────────────────┘  │
                                          │  ┌─────────────────────┐  │
                                          │  │ analytics-service   │  │
                                          │  │ daily order count   │  │
                                          │  │ revenue tracking    │  │
                                          │  │ top users (Redis)   │  │
                                          │  └─────────────────────┘  │
                                          │  ┌─────────────────────┐  │
                                          │  │ notification-service│  │
                                          │  │ queues BullMQ       │  │
                                          │  │ email jobs          │  │
                                          │  └─────────────────────┘  │
                                          └────────────────────────────┘
                          │
           ┌──────────────┼──────────────┐
           ▼              ▼              ▼
    PostgreSQL          Redis          Redis
    orders, users,     cache,         leaderboard,
    products,          locks,         analytics,
    order_items        rate limits    counters
```

---

## Project Structure

```
order-processing-system/
├── docker-compose.yml           # PostgreSQL, Redis, Kafka, Kafka UI
├── .env                         # Environment variables
├── package.json
├── tsconfig.json
│
└── src/
    ├── main.ts                  # Entry: HTTP server
    ├── workers.ts               # Entry: BullMQ workers
    ├── consumers.ts             # Entry: Kafka consumers
    ├── app.ts                   # Express app setup
    │
    ├── config/
    │   └── env.ts               # All environment config
    │
    ├── models/                  # TypeScript interfaces (no DB logic)
    │   ├── order.model.ts
    │   ├── product.model.ts
    │   └── user.model.ts
    │
    ├── db/                      # PostgreSQL layer
    │   ├── client.ts            # pg Pool singleton
    │   ├── migrate.ts           # Migration runner (idempotent)
    │   ├── seed.ts              # Seed script
    │   ├── migrations/
    │   │   └── 001_create_tables.sql
    │   └── repositories/
    │       ├── user.repository.ts
    │       ├── product.repository.ts   # Includes FOR UPDATE logic
    │       └── order.repository.ts
    │
    ├── redis/                   # Redis layer
    │   ├── client.ts            # redis client singleton
    │   ├── cache.repository.ts  # Generic get/set/delete with TTL
    │   ├── lock.repository.ts   # Distributed lock (SET NX PX)
    │   ├── leaderboard.repository.ts  # Sorted sets
    │   └── rateLimiter.ts       # Sliding window rate limiter
    │
    ├── kafka/                   # Kafka layer
    │   ├── producer.ts          # KafkaJS producer
    │   ├── consumer.ts          # Consumer group factory
    │   ├── topics.ts            # Topic names + event type definitions
    │   └── handlers/
    │       ├── order.handler.ts       # Inventory updates
    │       ├── analytics.handler.ts   # Metrics tracking
    │       └── notification.handler.ts # Queues email jobs
    │
    ├── queues/                  # BullMQ layer
    │   ├── email.queue.ts       # Email queue definition
    │   ├── order.queue.ts       # Order processing queue
    │   └── workers/
    │       ├── email.worker.ts  # Processes email jobs
    │       └── order.worker.ts  # Processes payment + fulfillment jobs
    │
    ├── services/                # Business logic
    │   ├── order.service.ts     # createOrder, cancelOrder, getOrder
    │   └── product.service.ts   # getProduct (with cache-aside)
    │
    ├── controllers/             # HTTP request handlers
    │   ├── order.controller.ts
    │   ├── product.controller.ts
    │   └── analytics.controller.ts
    │
    └── routes/
        └── index.ts             # All route definitions
```

---

## Database Schema

```sql
users
  id       TEXT PRIMARY KEY
  name     TEXT
  email    TEXT UNIQUE
  tier     TEXT  -- 'free' | 'premium' | 'vip'

products
  id       TEXT PRIMARY KEY
  name     TEXT
  price    NUMERIC(10,2)
  stock    INTEGER  CHECK (stock >= 0)   -- DB-level guard against going negative
  category TEXT

orders
  id           TEXT PRIMARY KEY
  user_id      TEXT  REFERENCES users(id)
  total_amount NUMERIC(10,2)
  status       TEXT  -- pending | confirmed | paid | shipped | cancelled | failed
  created_at   TIMESTAMPTZ
  updated_at   TIMESTAMPTZ

order_items
  id           SERIAL PRIMARY KEY
  order_id     TEXT  REFERENCES orders(id) ON DELETE CASCADE
  product_id   TEXT  REFERENCES products(id)
  product_name TEXT
  quantity     INTEGER
  unit_price   NUMERIC(10,2)
  total_price  NUMERIC(10,2)

schema_migrations
  filename   TEXT PRIMARY KEY      -- tracks which SQL files have already run
  applied_at TIMESTAMPTZ
```

---

## Redis Data Structures Used

| Key Pattern | Redis Type | Purpose | TTL |
|-------------|------------|---------|-----|
| `product:{id}` | String (JSON) | Product cache (cache-aside) | 5 min |
| `user-tier:{userId}` | String | User tier cache for rate limiter | 5 min |
| `lock:order-create:{userId}` | String | Distributed lock (SET NX PX) | 5 sec auto-expire |
| `ratelimit:{userId}` | Sorted Set | Sliding window — score = timestamp ms | 60 sec |
| `leaderboard:products` | Sorted Set | Sales ranking — score = total units sold | permanent |
| `analytics:orders:{date}` | String (counter) | Daily order count | permanent |
| `analytics:revenue:{date}` | String (counter) | Daily revenue in cents (integer safe) | permanent |
| `analytics:paid_orders:{date}` | String (counter) | Daily paid orders count | permanent |
| `analytics:cancelled_orders:{date}` | String (counter) | Daily cancellations count | permanent |
| `analytics:top_users` | Sorted Set | Users ranked by total orders | permanent |

---

## Kafka Topics

| Topic | Published By | Consumed By |
|-------|-------------|-------------|
| `order.events` | Order Service, Order Worker | inventory-service-group, analytics-service-group, notification-service-group |
| `inventory.events` | Inventory Handler | notification-service-group |
| `analytics.events` | (reserved for future use) | analytics-service-group |
| `notification.events` | (reserved for future use) | notification-service-group |

### Event Types

| Event | Trigger | Consumer Action |
|-------|---------|----------------|
| `order.created` | Customer places order | Inventory: cache invalidate + leaderboard; Analytics: increment counters; Notification: queue confirmation email |
| `order.paid` | BullMQ payment worker succeeds | Analytics: increment paid counter; Notification: queue paid email |
| `order.shipped` | BullMQ fulfillment worker | Notification: queue shipping email |
| `order.cancelled` | Customer cancels | Inventory: return stock to PostgreSQL (transaction); Cache: invalidate products |
| `inventory.low_stock` | Inventory handler detects <= 5 stock | Notification: queue admin alert email |

---

## BullMQ Queues

### email-queue
- **Concurrency:** 5 (5 emails processed simultaneously)
- **Retries:** 3 attempts with exponential backoff (2s → 4s → 8s)
- **On failure:** 10% simulated SMTP failure to demonstrate retry behavior
- **Keeps:** last 100 completed, last 50 failed jobs for debugging

### order-processing-queue
- **Concurrency:** 3
- **Retries:** 3 attempts with exponential backoff (3s → 6s → 12s)
- **Job flow:** `payment-processing` → (on success, after 2s delay) → `fulfillment`
- **On failure:** 5% simulated payment rejection to demonstrate retry behavior

---

## Seed Data

### Users (rate limits differ by tier)

| ID | Name | Email | Tier | Rate Limit |
|----|------|-------|------|------------|
| u1 | Alice Johnson | alice@example.com | vip | 100 req/min |
| u2 | Bob Smith | bob@example.com | premium | 30 req/min |
| u3 | Charlie Brown | charlie@example.com | free | 10 req/min |

### Products

| ID | Name | Price | Stock | Category |
|----|------|-------|-------|----------|
| p1 | iPhone 15 | $999.00 | 50 | electronics |
| p2 | MacBook Pro | $2499.00 | 20 | electronics |
| p3 | AirPods Pro | $249.00 | 100 | electronics |
| p4 | Nike Air Max | $150.00 | 200 | footwear |
| p5 | Sony PS5 | $499.00 | 15 | gaming |

---

## Getting Started

### Prerequisites

- Node.js 18+
- Docker + Docker Compose

### Step 1 — Install dependencies

```bash
cd order-processing-system
npm install
```

### Step 2 — Start infrastructure

```bash
docker-compose up -d
```

This starts:
- **PostgreSQL** on port `5432`
- **Redis** on port `6379`
- **Kafka** on port `9092`
- **Kafka UI** at `http://localhost:8080`

Wait about 15 seconds for Kafka to fully initialize before running the app.

### Step 3 — Seed the database (first time only)

```bash
npm run seed
```

Creates all tables via migrations and inserts users + products.

### Step 4 — Run all three processes (separate terminals)

```bash
# Terminal 1 — HTTP API + Kafka Producer
npm run dev

# Terminal 2 — BullMQ workers (payment + email)
npm run workers

# Terminal 3 — Kafka consumers (inventory, analytics, notifications)
npm run kafka:consumers
```

---

## API Reference and Curl Commands

All **order** endpoints require the `x-user-id` header. Product and analytics endpoints are public.

---

### Health Check

```bash
curl http://localhost:3000/health
```

**Response:**
```json
{
  "status": "ok",
  "timestamp": "2024-01-15T10:30:00.000Z"
}
```

---

### Products

#### List all products

```bash
curl http://localhost:3000/api/products
```

**Response:**
```json
{
  "products": [
    { "id": "p3", "name": "AirPods Pro",  "price": 249,  "stock": 100, "category": "electronics" },
    { "id": "p1", "name": "iPhone 15",    "price": 999,  "stock": 50,  "category": "electronics" },
    { "id": "p2", "name": "MacBook Pro",  "price": 2499, "stock": 20,  "category": "electronics" },
    { "id": "p4", "name": "Nike Air Max", "price": 150,  "stock": 200, "category": "footwear" },
    { "id": "p5", "name": "Sony PS5",     "price": 499,  "stock": 15,  "category": "gaming" }
  ]
}
```

#### Get a single product (demonstrates Redis cache-aside)

```bash
curl http://localhost:3000/api/products/p1
```

**First call:** Cache MISS — queries PostgreSQL, writes to Redis (5 min TTL).
**Second call:** Cache HIT — served from Redis in ~1ms, no DB query.

**Response:**
```json
{
  "product": {
    "id": "p1",
    "name": "iPhone 15",
    "price": 999,
    "stock": 50,
    "category": "electronics"
  }
}
```

**Response (not found):**
```json
{ "error": "Product not found" }
```

#### Get sales leaderboard (Redis sorted set)

```bash
curl http://localhost:3000/api/products/leaderboard
```

**Response:**
```json
{
  "leaderboard": [
    { "productId": "p1", "sales": 5, "rank": 1 },
    { "productId": "p3", "sales": 3, "rank": 2 },
    { "productId": "p4", "sales": 1, "rank": 3 }
  ]
}
```

#### Get a product's current sales rank

```bash
curl http://localhost:3000/api/products/p1/rank
```

**Response:**
```json
{
  "productId": "p1",
  "rank": 1
}
```

**Response (never ordered):**
```json
{
  "productId": "p2",
  "rank": "unranked"
}
```

---

### Orders

#### Place an order — single item

```bash
curl -X POST http://localhost:3000/api/orders \
  -H "Content-Type: application/json" \
  -H "x-user-id: u1" \
  -d '{
    "items": [
      { "productId": "p1", "quantity": 1 }
    ]
  }'
```

**Response (201):**
```json
{
  "success": true,
  "order": {
    "id": "ord_a1b2c3d4",
    "userId": "u1",
    "items": [
      {
        "productId": "p1",
        "productName": "iPhone 15",
        "quantity": 1,
        "unitPrice": 999,
        "totalPrice": 999
      }
    ],
    "totalAmount": 999,
    "status": "confirmed",
    "createdAt": "2024-01-15T10:30:00.000Z",
    "updatedAt": "2024-01-15T10:30:00.000Z"
  }
}
```

#### Place an order — multiple items

```bash
curl -X POST http://localhost:3000/api/orders \
  -H "Content-Type: application/json" \
  -H "x-user-id: u2" \
  -d '{
    "items": [
      { "productId": "p2", "quantity": 1 },
      { "productId": "p3", "quantity": 2 },
      { "productId": "p4", "quantity": 1 }
    ]
  }'
```

**Response (201):**
```json
{
  "success": true,
  "order": {
    "id": "ord_x9y8z7w6",
    "userId": "u2",
    "items": [
      { "productId": "p2", "productName": "MacBook Pro",  "quantity": 1, "unitPrice": 2499, "totalPrice": 2499 },
      { "productId": "p3", "productName": "AirPods Pro",  "quantity": 2, "unitPrice": 249,  "totalPrice": 498  },
      { "productId": "p4", "productName": "Nike Air Max", "quantity": 1, "unitPrice": 150,  "totalPrice": 150  }
    ],
    "totalAmount": 3147,
    "status": "confirmed",
    "createdAt": "2024-01-15T10:31:00.000Z",
    "updatedAt": "2024-01-15T10:31:00.000Z"
  }
}
```

#### Get an order by ID

```bash
curl http://localhost:3000/api/orders/ord_a1b2c3d4 \
  -H "x-user-id: u1"
```

**Response:**
```json
{
  "order": {
    "id": "ord_a1b2c3d4",
    "userId": "u1",
    "items": [
      { "productId": "p1", "productName": "iPhone 15", "quantity": 1, "unitPrice": 999, "totalPrice": 999 }
    ],
    "totalAmount": 999,
    "status": "shipped",
    "createdAt": "2024-01-15T10:30:00.000Z",
    "updatedAt": "2024-01-15T10:35:00.000Z"
  }
}
```

Note: Poll this endpoint every few seconds after placing an order to watch the status change: `confirmed → paid → shipped`.

#### Cancel an order

```bash
curl -X DELETE http://localhost:3000/api/orders/ord_a1b2c3d4 \
  -H "x-user-id: u1"
```

**Response:**
```json
{
  "success": true,
  "order": {
    "id": "ord_a1b2c3d4",
    "status": "cancelled",
    "updatedAt": "2024-01-15T10:32:00.000Z"
  }
}
```

This triggers a Kafka `order.cancelled` event. The inventory consumer will return stock to PostgreSQL in a database transaction and invalidate the Redis cache.

**Response (cannot cancel):**
```json
{ "error": "Cannot cancel order in status: shipped" }
```

---

### Analytics

#### Daily stats

```bash
# Today
curl http://localhost:3000/api/analytics/daily

# Specific date
curl "http://localhost:3000/api/analytics/daily?date=2024-01-15"
```

**Response:**
```json
{
  "date": "2024-01-15",
  "stats": {
    "orders": 12,
    "revenue": 14879.50,
    "paidOrders": 11,
    "cancelledOrders": 1
  }
}
```

#### Top users by order count

```bash
curl http://localhost:3000/api/analytics/top-users
```

**Response:**
```json
{
  "topUsers": [
    { "userId": "u1", "orders": 5 },
    { "userId": "u2", "orders": 3 },
    { "userId": "u3", "orders": 1 }
  ]
}
```

---

### Error Responses

#### Insufficient stock

```bash
curl -X POST http://localhost:3000/api/orders \
  -H "Content-Type: application/json" \
  -H "x-user-id: u1" \
  -d '{"items": [{"productId": "p5", "quantity": 999}]}'
```

**Response (500):**
```json
{
  "error": "Insufficient stock for \"Sony PS5\". Available: 15, Requested: 999"
}
```

#### Missing user ID header

```bash
curl -X POST http://localhost:3000/api/orders \
  -H "Content-Type: application/json" \
  -d '{"items": [{"productId": "p1", "quantity": 1}]}'
```

**Response (401):**
```json
{ "error": "Missing x-user-id header" }
```

#### Product not found

```bash
curl http://localhost:3000/api/products/does-not-exist
```

**Response (404):**
```json
{ "error": "Product not found" }
```

#### Order not found

```bash
curl http://localhost:3000/api/orders/ord_invalid \
  -H "x-user-id: u1"
```

**Response (404):**
```json
{ "error": "Order not found" }
```

#### Rate limit exceeded

Free tier users are limited to 10 requests per minute. Test it:

```bash
# Hit the limit as a free user (u3)
for i in {1..12}; do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    http://localhost:3000/api/products/p1 \
    -H "x-user-id: u3")
  echo "Request $i: HTTP $STATUS"
done
```

After request 10, you'll get `HTTP 429`:
```json
{
  "error": "Too many requests",
  "message": "Rate limit exceeded. Please slow down.",
  "retryAfter": "60 seconds"
}
```

Response headers show your current limit:
```
X-RateLimit-Limit: 10
X-RateLimit-Remaining: 7
```

---

## Full Order Flow Test (Step by Step)

Run this sequence to observe all 4 technologies working together:

```bash
# 1. Check initial stock of PS5
curl http://localhost:3000/api/products/p5
# stock: 15

# 2. Place an order (watch all 3 terminals for real-time logs)
ORDER=$(curl -s -X POST http://localhost:3000/api/orders \
  -H "Content-Type: application/json" \
  -H "x-user-id: u1" \
  -d '{"items": [{"productId": "p5", "quantity": 3}]}')

echo $ORDER
ORDER_ID=$(echo $ORDER | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)
echo "Order ID: $ORDER_ID"

# 3. Stock was deducted from PostgreSQL (15 - 3 = 12)
curl http://localhost:3000/api/products/p5
# stock: 12

# 4. Poll order status — watch it change: confirmed → paid → shipped
curl http://localhost:3000/api/orders/$ORDER_ID -H "x-user-id: u1"
# Wait ~5 seconds and run again to see status change

# 5. Check leaderboard updated in Redis
curl http://localhost:3000/api/products/leaderboard
# p5 appears with sales: 3

# 6. Check analytics counters in Redis
curl http://localhost:3000/api/analytics/daily
# orders: 1, revenue: 1497

# 7. Cancel the order (only works while status is confirmed or payment_processing)
curl -X DELETE http://localhost:3000/api/orders/$ORDER_ID \
  -H "x-user-id: u1"

# 8. Stock returned to PostgreSQL via transaction
curl http://localhost:3000/api/products/p5
# stock: 15 again (Redis cache was invalidated, reads fresh from PostgreSQL)
```

---

## What Happens When You Place an Order (Internals)

```
POST /api/orders
│
├─ [Redis] Rate limiter: ZREMRANGEBYSCORE + ZCARD + ZADD (~1ms)
│   sliding window check — rejects if over tier limit
│
└─ OrderService.createOrder()
    │
    ├─ [PostgreSQL] findUserById("u1")  ~2ms
    │
    ├─ [Redis] SET NX PX — acquire distributed lock  ~1ms
    │   Key:   lock:order-create:u1
    │   Value: random UUID (ownership token)
    │   TTL:   5 seconds (auto-releases if process crashes)
    │
    ├─ [PostgreSQL] pool.connect() → BEGIN
    │
    ├─ [PostgreSQL] SELECT products FOR UPDATE  ~3ms
    │   Locks product rows — other transactions WAIT here
    │   ORDER BY id prevents deadlocks
    │
    ├─ Validate: stock >= requested quantity
    │
    ├─ [PostgreSQL] UPDATE products SET stock = stock - 3  ~2ms
    │
    ├─ [PostgreSQL] INSERT INTO orders ...  ~2ms
    ├─ [PostgreSQL] INSERT INTO order_items ...  ~2ms
    │
    ├─ [PostgreSQL] COMMIT  ~1ms
    │   stock deduction + order creation are ATOMIC
    │   if order insert fails → stock rollback happens automatically
    │
    ├─ client.release() → back to connection pool
    │
    ├─ [Redis] DEL product:p5  ~1ms  (invalidate stale cache)
    │
    ├─ [Kafka] producer.send(order.events, "order.created")  ~5ms
    │   Partition key: order ID (same order → same partition)
    │
    └─ [BullMQ] orderQueue.add("payment-processing")  ~2ms

API responds with order object  ~20ms total

ASYNC — Kafka consumers process "order.created" in parallel:
│
├─ inventory-service-group:
│   ├─ DEL product:p5  (cache invalidation)
│   ├─ ZINCRBY leaderboard:products 3 "p5"  (leaderboard)
│   ├─ findProductById("p5")  (check current stock)
│   └─ (if stock <= 5) → publish inventory.low_stock event
│
├─ analytics-service-group:
│   ├─ INCR analytics:orders:2024-01-15
│   ├─ INCRBY analytics:revenue:2024-01-15  149700  (cents)
│   └─ ZINCRBY analytics:top_users 1 "u1"
│
└─ notification-service-group:
    ├─ findUserById("u1") → alice@example.com
    └─ emailQueue.add({ to: "alice@example.com", template: "order_confirmation" })

ASYNC — BullMQ workers process jobs:
│
├─ email.worker (concurrency: 5):
│   ├─ Simulate SMTP 500ms-1.5s
│   ├─ (10% chance) SMTP timeout → retry with backoff
│   └─ "Email sent to alice@example.com"
│
└─ order.worker (concurrency: 3):
    ├─ job: payment-processing
    │   ├─ Simulate payment gateway 1-2s
    │   ├─ (5% chance) Gateway rejected → retry with backoff
    │   ├─ updateOrderStatus("ord_abc", "paid")  PostgreSQL
    │   ├─ publishEvent(order.events, "order.paid")  Kafka
    │   └─ orderQueue.add("fulfillment", { delay: 2000ms })
    │
    └─ job: fulfillment  (2 seconds later)
        ├─ Simulate pick & pack 2-3s
        ├─ updateOrderStatus("ord_abc", "shipped")  PostgreSQL
        └─ publishEvent(order.events, "order.shipped")  Kafka
```

---

## Monitoring

### Kafka UI — http://localhost:8080

See live messages flowing through topics, consumer group lag, and partition details.

### Redis CLI

```bash
docker exec -it ops-redis redis-cli

# Watch all Redis commands in real-time (place an order while this is open)
MONITOR

# Check product cache
GET product:p1

# Check leaderboard
ZREVRANGE leaderboard:products 0 -1 WITHSCORES

# Check rate limit entries for a user
ZRANGE ratelimit:u1 0 -1 WITHSCORES

# Check if a lock is currently held
GET lock:order-create:u1

# Check analytics counters
GET analytics:orders:2024-01-15
GET analytics:revenue:2024-01-15

# Check top users ranking
ZREVRANGE analytics:top_users 0 -1 WITHSCORES
```

### PostgreSQL

```bash
docker exec -it ops-postgres psql -U ops_user -d ops_db

-- All orders, newest first
SELECT id, user_id, total_amount, status, created_at
FROM orders
ORDER BY created_at DESC;

-- Order with all its items
SELECT o.id, o.status, oi.product_name, oi.quantity, oi.unit_price, oi.total_price
FROM orders o
JOIN order_items oi ON o.id = oi.order_id
WHERE o.id = 'ord_xxxxxxxx';

-- Current stock levels
SELECT id, name, stock FROM products ORDER BY name;

-- Revenue by status
SELECT status, COUNT(*) as count, SUM(total_amount) as revenue
FROM orders
GROUP BY status;

-- Migration history
SELECT * FROM schema_migrations ORDER BY applied_at;
```

---

## Key Concepts Explained

### Why Redis lock AND PostgreSQL FOR UPDATE?

These solve two different concurrency problems:

**Redis lock** (`SET NX PX`) prevents the **same-user double-submit** problem:
- Alice double-clicks "Buy Now" and sends two identical requests
- Both hit the API simultaneously and try to start a transaction
- The Redis lock lets only one proceed — the other gets an error immediately
- Fast (~1ms), no DB involved

**PostgreSQL FOR UPDATE** prevents the **cross-user oversell** problem:
- Alice (app instance 1) and Bob (app instance 2) both want the last PS5 (stock: 1)
- Each has their own Redis lock (different users, different lock keys)
- Without FOR UPDATE: both read stock=1, both pass validation, both deduct → stock becomes -1 (oversell!)
- With FOR UPDATE: Bob's transaction waits at the SELECT until Alice's COMMIT. Then Bob reads stock=0 and gets "Insufficient stock"

**Both are necessary.** Neither alone is sufficient in a multi-instance deployment.

### Why is the order flow synchronous but email is async?

**Order creation is synchronous** because the customer needs immediate confirmation. Stock must be deducted before responding — if two customers both see "1 left" and both click Buy, only one should succeed.

**Email sending is async** (BullMQ) because:
- SMTP can take 500ms-2s — unacceptable API latency
- Email delivery can fail (SMTP timeout) — BullMQ retries automatically
- The customer already has their order confirmation in the response

### Why Kafka instead of calling services directly?

Without Kafka, order creation would directly call: inventory service, analytics service, notification service. Problems:
- Adding loyalty points service means modifying order service code
- A bug in analytics crashes the order creation
- All services are tightly coupled — changing one affects others

With Kafka, order service publishes one `order.created` event. Any number of independent consumers react to it. Adding loyalty points = add a new consumer group. The order service never changes.

### Cache-aside pattern

```
GET /api/products/p1

1. GET product:p1 from Redis
   → HIT:  return JSON (no DB query, ~1ms)
   → MISS: SELECT from PostgreSQL (~3ms)
           SETEX product:p1 300 <json>  (cache for 5 min)
           return data

On stock change (order placed or cancelled):
   DEL product:p1  → next request goes to PostgreSQL for fresh data
```

### Why store revenue in cents?

```
INCRBY analytics:revenue:2024-01-15  149700
```

Redis `INCR`/`INCRBY` only work on integers. Storing `$1497.00` as `149700 cents` avoids floating point precision issues and lets us use atomic integer operations. We divide by 100 when reading.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | localhost | PostgreSQL host |
| `DB_PORT` | 5432 | PostgreSQL port |
| `DB_NAME` | ops_db | Database name |
| `DB_USER` | ops_user | Database user |
| `DB_PASSWORD` | ops_pass | Database password |
| `DB_POOL_MAX` | 10 | Max PostgreSQL connections in pool |
| `REDIS_HOST` | localhost | Redis host |
| `REDIS_PORT` | 6379 | Redis port |
| `REDIS_PASSWORD` | (empty) | Redis password (optional) |
| `KAFKA_BROKERS` | localhost:9092 | Comma-separated broker addresses |
| `KAFKA_CLIENT_ID` | order-processing-system | Kafka client identifier |
| `KAFKA_GROUP_ID` | order-group | Default consumer group ID |
| `PORT` | 3000 | HTTP server port |
| `NODE_ENV` | development | Environment |

---

## npm Scripts

| Command | What It Does |
|---------|-------------|
| `npm run dev` | Start HTTP API server + Kafka producer |
| `npm run workers` | Start BullMQ email and order workers |
| `npm run kafka:consumers` | Start all 3 Kafka consumer groups |
| `npm run seed` | Run migrations + insert seed data |

---

## What's NOT Covered ❌

Gaps if you want deeper mastery:

| Topic | What's Missing |
|-------|---------------|
| Redis Pub/Sub | Real-time channel subscriptions — different from Kafka |
| Redis Streams | Persistent log with consumer groups (like Kafka but built into Redis) |
| Redis Transactions (MULTI/EXEC) | Atomic pipelines |
| Redis Pipeline / batch | Sending multiple commands in one round trip |
| Redis Lua scripts | Atomic custom logic (used in advanced rate limiters) |
| Redis Hash type | Storing objects field-by-field instead of JSON strings |
| Redis Lists | Simple queue/stack (LPUSH/RPOP) |
| BullMQ QueueEvents | Listening to queue-level events (progress, stalled) |
| BullMQ Flow / Parent-Child jobs | Complex job dependency graphs |
| Kafka transactions | Exactly-once semantics across produce + consume |
| Kafka schema registry | Avro/Protobuf schema enforcement |
| Kafka offset management | Manual commit, seek to offset |
