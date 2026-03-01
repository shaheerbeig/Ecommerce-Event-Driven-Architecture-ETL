-- Minimal Gold layer analytics schema
-- Only fact tables used by silver_to_gold.py

CREATE SCHEMA IF NOT EXISTS analytics;

-- Fact table: All events enriched with dimensions (written by silver_to_gold.py)
CREATE TABLE IF NOT EXISTS analytics.facts_events (
    event_id           TEXT PRIMARY KEY,
    event_type         TEXT NOT NULL,
    event_timestamp    TIMESTAMP NOT NULL,
    ingest_timestamp   TIMESTAMP NOT NULL,
    event_date         DATE NOT NULL,
    event_hour         INT NOT NULL,
    
    user_id            TEXT,
    user_country       TEXT,
    user_city          TEXT,
    user_signup_channel TEXT,
    user_device_preference TEXT,
    user_created_at    TIMESTAMP,
    user_is_active     BOOLEAN,
    
    product_id         TEXT,
    product_category   TEXT,
    product_brand      TEXT,
    product_price      NUMERIC(10,2),
    product_currency   TEXT,
    product_inventory_status TEXT,
    
    session_id         TEXT,
    device_type        TEXT,
    ip                 TEXT,
    country            TEXT,
    city               TEXT,
    
    quantity           INT,
    cart_value         NUMERIC(10,2),
    cart_item_count    INT,
    order_id           TEXT,
    amount             NUMERIC(12,2),
    currency           TEXT,
    payment_method     TEXT,
    search_term        TEXT,
    results_count      INT,
    
    processing_latency_seconds INT
);


-- Fact table: Purchase events only (written by silver_to_gold.py)
CREATE TABLE IF NOT EXISTS analytics.facts_purchases (
    purchase_event_id      TEXT PRIMARY KEY,
    event_date             DATE NOT NULL,
    event_hour             INT NOT NULL,
    purchase_timestamp     TIMESTAMP NOT NULL,
    order_id               TEXT NOT NULL,
    
    user_id                TEXT NOT NULL,
    user_signup_channel    TEXT,
    user_country           TEXT,
    
    product_id             TEXT,
    product_category       TEXT,
    product_brand          TEXT,
    
    purchase_amount        NUMERIC(12,2) NOT NULL,
    currency               TEXT NOT NULL,
    payment_method         TEXT,
    
    session_id             TEXT,
    device_type            TEXT,
    user_device_preference TEXT,
    
    purchase_date          DATE NOT NULL,
    purchase_hour          INT NOT NULL
);


-- Fact table: Aggregated session metrics (written by silver_to_gold.py)
CREATE TABLE IF NOT EXISTS analytics.facts_user_sessions (
    session_id                 TEXT,
    user_id                    TEXT,
    event_date                 DATE NOT NULL,
    event_hour                 INT NOT NULL,
    
    user_signup_channel        TEXT,
    user_country               TEXT,
    device_type                TEXT,
    
    event_count                INT NOT NULL,
    unique_products_viewed     INT,
    product_views              INT,
    add_to_cart_count          INT,
    purchase_count             INT,
    
    max_purchase_amount        NUMERIC(12,2),
    total_purchase_amount      NUMERIC(12,2),
    
    session_start_time         TIMESTAMP NOT NULL,
    session_end_time           TIMESTAMP NOT NULL,
    session_duration_minutes   NUMERIC(10,2),
    
    conversion_rate            NUMERIC(5,4),
    
    PRIMARY KEY (event_date, session_id, user_id)
);
