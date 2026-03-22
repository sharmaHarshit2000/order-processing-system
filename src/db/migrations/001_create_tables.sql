CREATE TABLE IF NOT EXISTS users (
  id       TEXT PRIMARY KEY,
  name     TEXT NOT NULL,
  email    TEXT NOT NULL UNIQUE,
  tier     TEXT NOT NULL CHECK (tier IN ('free', 'premium', 'vip'))
);

CREATE TABLE IF NOT EXISTS products (
  id       TEXT PRIMARY KEY,
  name     TEXT NOT NULL,
  price    NUMERIC(10, 2) NOT NULL,
  stock    INTEGER NOT NULL CHECK (stock >= 0),
  category TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
  id           TEXT PRIMARY KEY,
  user_id      TEXT NOT NULL REFERENCES users(id),
  total_amount NUMERIC(10, 2) NOT NULL,
  status       TEXT NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
  id           SERIAL PRIMARY KEY,
  order_id     TEXT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  product_id   TEXT NOT NULL REFERENCES products(id),
  product_name TEXT NOT NULL,
  quantity     INTEGER NOT NULL,
  unit_price   NUMERIC(10, 2) NOT NULL,
  total_price  NUMERIC(10, 2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id    ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
