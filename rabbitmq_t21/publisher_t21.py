import json
import aio_pika
from aio_pika import Exchange

class RabbitmqPublisherTask21:


    def __init__(self, exchange: Exchange) -> None:
        self.exchange = exchange


    async def publish(self, message: dict) -> None:
        await self.exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(), 
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="task21.key",
        )
        print("Message task21 is published...")