import json
import aio_pika
from uuid import uuid4
from aio_pika import Exchange
from datetime import datetime, timezone


class RabbitmqPublisherTask30:


    def __init__(self, exchange: Exchange) -> None:
        self.exchange = exchange
        self.cor_id = str(uuid4())


    async def publish(self, message: dict, routing_key: str) -> None:
        await self.exchange.publish(
            message=aio_pika.Message(
                body=json.dumps(message.get("body")).encode(),
                headers=message.get("headers"),
                correlation_id=self.cor_id,
                timestamp=datetime.now(timezone.utc),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=routing_key
        )
        print(f"Message is published at routing_key: {routing_key}")