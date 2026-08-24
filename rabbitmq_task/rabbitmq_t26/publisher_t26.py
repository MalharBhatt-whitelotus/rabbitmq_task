import json
import aio_pika
from aio_pika import Exchange

class RabbitmqPublisherTask26:


    def __init__(self, exchange: Exchange) -> None:
        self.exchange = exchange


    async def publish(self, message: dict) -> None:
        await self.exchange.publish(
            message=aio_pika.Message(body=json.dumps(message).encode()),
            routing_key="main_task26.key",
        )
        print("Message published....")