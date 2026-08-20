import json
import aio_pika
from aio_pika import Exchange

class RabbitmqPublisherTask22:


    def __init__(self, exchange: Exchange) -> None:
        self.exchange = exchange


    async def publish(self, message: dict, routing_key: str) -> None:
        await self.exchange.publish(
            message=aio_pika.Message(
                body=json.dumps(message).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=routing_key,
        )
        print("Message task22 published...")