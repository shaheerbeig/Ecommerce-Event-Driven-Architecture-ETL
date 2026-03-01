# dimensions/seed_postgres.py

import uuid
import random
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from faker import Faker
import psycopg2
from psycopg2.extras import execute_values


CONFIG_PATH = Path("config/app_config.yaml")


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_conn(pg_cfg: dict):
    return psycopg2.connect(
        host=pg_cfg["host"],
        port=pg_cfg["port"],
        dbname=pg_cfg["database"],
        user=pg_cfg["user"],
        password=pg_cfg["password"],
    )


def seed_users(cur, n_users: int):
    fake = Faker()

    countries = ["PK", "IN", "US", "AE", "UK"]
    cities_pk = ["Karachi", "Lahore", "Islamabad", "Rawalpindi", "Faisalabad"]
    signup_channels = ["organic", "ads", "referral"]
    device_prefs = ["web", "android", "ios"]

    now = datetime.utcnow()
    rows = []
    for _ in range(n_users):
        user_id = f"U_{uuid.uuid4().hex[:12]}"
        created_at = now - timedelta(days=random.randint(0, 120), minutes=random.randint(0, 1440))
        # updated_at >= created_at
        updated_at = created_at + timedelta(days=random.randint(0, 30), minutes=random.randint(0, 1440))
        country = random.choice(countries)
        city = random.choice(cities_pk) if country == "PK" else fake.city()
        signup_channel = random.choice(signup_channels)
        device_preference = random.choice(device_prefs)
        is_active = random.random() > 0.05  # 95% active

        rows.append(
            (user_id, created_at, updated_at, country, city, signup_channel, device_preference, is_active)
        )

    sql = """
        INSERT INTO app_users
        (user_id, created_at, updated_at, country, city, signup_channel, device_preference, is_active)
        VALUES %s
        ON CONFLICT (user_id) DO NOTHING
    """
    execute_values(cur, sql, rows, page_size=1000)


def seed_products(cur, n_products: int):
    categories = ["Shoes", "Watches", "Clothing", "Electronics", "Bags", "Beauty"]
    brands = ["Acme", "Orion", "Nimbus", "Vertex", "Nova", "Zenith"]
    inventory_statuses = ["in_stock", "out_of_stock", "limited"]
    currency = "PKR"

    now = datetime.utcnow()
    rows = []
    for _ in range(n_products):
        product_id = f"P_{uuid.uuid4().hex[:12]}"
        created_at = now - timedelta(days=random.randint(0, 365), minutes=random.randint(0, 1440))
        updated_at = created_at + timedelta(days=random.randint(0, 60), minutes=random.randint(0, 1440))

        category = random.choice(categories)
        brand = random.choice(brands)
        price = round(random.uniform(300.0, 50000.0), 2)
        is_active = random.random() > 0.03  # 97% active
        inventory_status = random.choice(inventory_statuses)

        rows.append((product_id, created_at, updated_at, category, brand, price, is_active, currency, inventory_status))

    sql = """
        INSERT INTO app_products
        (product_id, created_at, updated_at, category, brand, price, is_active, currency, inventory_status)
        VALUES %s
        ON CONFLICT (product_id) DO NOTHING
    """
    execute_values(cur, sql, rows, page_size=1000)


def main():
    cfg = load_config()
    pg = cfg["postgres"]

    n_users = 1000
    n_products = 500

    conn = get_conn(pg)
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            seed_users(cur, n_users=n_users)
            seed_products(cur, n_products=n_products)

        conn.commit()
        print(f"✅ Seed complete: users={n_users}, products={n_products}")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
