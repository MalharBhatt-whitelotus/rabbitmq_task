import json
import aio_pika
from uuid import uuid4
from aio_pika import Exchange

class RabbitmqPublisherTask24:


    def __init__(self, exchange: Exchange):
        self.exchange = exchange
        self.uuid = str(uuid4())


    async def publish(self, message: dict, routing_key: str) -> None:
        await self.exchange.publish(
            message=aio_pika.Message(
                body=json.dumps(message.get("body")).encode(),
                headers=message.get("headers"),
                correlation_id=self.uuid,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=routing_key,
        )
        print(f"Published message {self.uuid} to routing key: {routing_key}")