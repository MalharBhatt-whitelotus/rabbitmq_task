import json
import aio_pika
from uuid import uuid4
from aio_pika import Exchange
from datetime import datetime, timezone


class RabbitmqPublisherTask27:


    def __init__(self, exchange: Exchange) -> None:
        self.exchange = exchange
        self.uuid = str(uuid4())


    async def publish(self, message: dict, routing_key: str) -> None:
        await self.exchange.publish(
            message=aio_pika.Message(
                body=json.dumps(message.get("body")).encode(),
                headers=message.get("headers"),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                correlation_id=self.uuid,
                timestamp=datetime.now(timezone.utc)
            ),
            routing_key=routing_key,
        )