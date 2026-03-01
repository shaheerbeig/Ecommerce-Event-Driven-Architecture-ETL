import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
import psycopg2
import yaml
from publisher import RabbitMQPublisher

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "app_config.yaml"

def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def fetch_ids(pg_cfg: dict, limit_users=500, limit_products=500):

    conn = psycopg2.connect(
        host=pg_cfg["host"],
        port=pg_cfg["port"],
        dbname=pg_cfg["database"],
        user=pg_cfg["user"],
        password=pg_cfg["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM app_users WHERE is_active = true LIMIT %s;", (limit_users,))
            users = [r[0] for r in cur.fetchall()]

            cur.execute("SELECT product_id FROM app_products WHERE is_active = true LIMIT %s;", (limit_products,))
            products = [r[0] for r in cur.fetchall()]

        return users, products
    finally:
        conn.close()


def make_base_event(event_type: str, user_id: str, session_id: str):
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "user_id": user_id,
        "session_id": session_id,

        # extra envelope/meta
        "ingest_source": "local_producer_v1",
        "device_type": random.choice(["web", "android", "ios"]),
        "ip": f"10.0.{random.randint(0,255)}.{random.randint(1,254)}",
        "user_agent": "demo-agent",
        "country": "PK",
        "city": random.choice(["Karachi", "Lahore", "Islamabad"]),

        # event-specific fields (nullable unless needed)
        "product_id": None,
        "quantity": None,
        "cart_value": None,
        "cart_item_count": None,
        "order_id": None,
        "amount": None,
        "currency": None,
        "payment_method": None,
        "items": None,
        "search_term": None,
        "results_count": None,
    }


def main():
    cfg = load_config()

    users, products = fetch_ids(cfg["postgres"])
    if not users or not products:
        raise RuntimeError("No users/products found. Seeding must be done before running the producer.")

    publisher = RabbitMQPublisher(
        host=cfg["rabbitmq"]["host"],
        port=cfg["rabbitmq"]["port"],
        user=cfg["rabbitmq"]["user"],
        password=cfg["rabbitmq"]["password"],
        exchange="events_exchange",
        routing_key="clickstream",
        queue_name=cfg["rabbitmq"]["queue_clickstream"],
    )

    print("Producer started. Publishing events to RabbitMQ...")

    try:
        while True:
            print("Generating new event batch...")
            user_id = random.choice(users)
            session_id = f"S_{uuid.uuid4().hex[:10]}"
            product_id = random.choice(products)

            # 1) product_view
            e1 = make_base_event("product_view", user_id, session_id)
            e1["product_id"] = product_id
            publisher.publish(e1)

            time.sleep(0.2)

            # 2) add_to_cart
            e2 = make_base_event("add_to_cart", user_id, session_id)
            e2["product_id"] = product_id
            e2["quantity"] = 1
            publisher.publish(e2)

            time.sleep(0.2)

            # 3) sometimes purchase
            if random.random() < 0.35:
                e3 = make_base_event("purchase", user_id, session_id)
                e3["order_id"] = f"O_{uuid.uuid4().hex[:10]}"
                e3["amount"] = round(random.uniform(300, 50000), 2)
                e3["currency"] = "PKR"
                e3["payment_method"] = random.choice(["cod", "card", "wallet"])
                publisher.publish(e3)

            time.sleep(0.4)

    finally:
        publisher.close()


if __name__ == "__main__":
    main()
