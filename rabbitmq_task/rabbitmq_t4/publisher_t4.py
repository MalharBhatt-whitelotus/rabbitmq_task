import json
import aio_pika

class RabbitmqPublisherTask4:

    def __init__(self, channel):
        self.channel = channel

    async def publish(self, message: dict):
        await self.channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(message).encode("utf-8")),
            routing_key = "test_queue4",
        )
        print("Messages task 4 published.")