import json
import aio_pika

class RabbitmqPublisherTask11:

    def __init__(self, exchange):
        self.exchange = exchange

    async def publish(self, message):
        await self.exchange.publish(
            aio_pika.Message(json.dumps(message).encode("utf-8")),
            routing_key = "file.uploaded",
        )
        print("Message published...")