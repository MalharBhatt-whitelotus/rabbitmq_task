import json
import aio_pika

class RabbitmqPublisherTask15:

    def __init__(self, exchange):
        self.exchange = exchange

    async def publish(self, message):
        await self.exchange.publish(
            aio_pika.Message(json.dumps(message).encode()),
            routing_key = "grace_shut",
        )
        print("Message task15 published...")