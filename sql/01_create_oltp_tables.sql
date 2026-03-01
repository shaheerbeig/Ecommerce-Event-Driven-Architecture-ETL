CREATE TABLE IF NOT EXISTS app_users (
  user_id           TEXT PRIMARY KEY,
  created_at        TIMESTAMP NOT NULL,
  updated_at        TIMESTAMP NOT NULL,
  country           TEXT NOT NULL,
  city              TEXT NOT NULL,
  signup_channel    TEXT NOT NULL,
  device_preference TEXT NOT NULL,
  is_active         BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS app_products (
  product_id        TEXT PRIMARY KEY,
  created_at        TIMESTAMP NOT NULL,
  updated_at        TIMESTAMP NOT NULL,
  category          TEXT NOT NULL,
  brand             TEXT NOT NULL,
  price             NUMERIC(10,2) NOT NULL,
  is_active         BOOLEAN NOT NULL,
  currency          TEXT NOT NULL,
  inventory_status  TEXT NOT NULL
);
