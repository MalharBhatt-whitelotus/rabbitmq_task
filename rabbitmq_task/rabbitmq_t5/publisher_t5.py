import json
import aio_pika
from aio_pika.abc import AbstractIncomingMessage


class RabbitmqPublisherTask5:

    def __init__(self, channel):
        self.channel = channel

    async def publish(self, *message: AbstractIncomingMessage):
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(message).encode("utf-8")),
            routing_key = "test_queue5",
        )
        print("Message task 5 published.")