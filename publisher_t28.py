import json
import aio_pika
from aio_pika import Exchange


class RabbitmqPublisherTask28:


    def __init__(self, exchange: Exchange) -> None:
        self.exchange = exchange


    async def publisher(self, message: dict) -> None:
        await self.exchange.publish(
            message=aio_pika.Message(
                body=json.dumps(message).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="main_task28.key",
        )