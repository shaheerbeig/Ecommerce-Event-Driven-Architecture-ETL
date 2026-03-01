import json
import pika


class RabbitMQPublisher:
    def __init__(self, host: str, port: int, user: str, password: str,
                 exchange: str, routing_key: str, queue_name: str):
        creds = pika.PlainCredentials(user, password)
        params = pika.ConnectionParameters(host=host, port=port, credentials=creds)
        self.conn = pika.BlockingConnection(params)
        self.channel = self.conn.channel()

        self.exchange = exchange
        self.channel.exchange_declare(exchange=self.exchange, exchange_type="direct", durable=True)

        # 2) Create queue
        self.queue = queue_name
        self.channel.queue_declare(queue=self.queue, durable=True)

        # 3) Bind queue to exchange with routing key
        self.routing_key = routing_key
        self.channel.queue_bind(exchange=self.exchange, queue=self.queue, routing_key=self.routing_key)

    def publish(self, message: dict):
        body = json.dumps(message).encode("utf-8")
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing_key,
            body=body,
            properties=pika.BasicProperties(
                delivery_mode=2,                # persistent
                content_type="application/json"
            ),
        )

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass
