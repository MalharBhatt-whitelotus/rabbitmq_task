import json
import aio_pika

class RabbitmqPublisherTask2:

    def __init__(self, channel):
        self.channel = channel

    async def publish(self, *messages: dict):
        for message in messages:
            await self.channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps(message).encode("utf-8")),
                routing_key = "test_queue2",
            )
        print("Message published.")