import json
import aio_pika

class RabbitmqPublisherTask7:

    def __init__(self, channel):
        self.channel = channel

    async def publish(self, message):
        await self.channel.default_exchange.publish(
            aio_pika.Message(json.dumps(message).encode("utf-8")),
            routing_key = "test_queue7",
        )
        print("message task6 published...")