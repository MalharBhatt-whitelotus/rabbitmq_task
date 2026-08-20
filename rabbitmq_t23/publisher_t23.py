import json
import aio_pika
from aio_pika import Exchange

class RabbitmqPublisherTask23:


    def __init__(self, exchange: Exchange) -> None:
        self.exchange = exchange


    async def publish(self, message: dict, routing_key: str) -> None:
        await self.exchange.publish(
            message=aio_pika.Message(
                body=json.dumps(message.get("body")).encode(),
                headers=message.get("headers"),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=routing_key,
        )
        print(f"Message task23 Published at routing={routing_key}")