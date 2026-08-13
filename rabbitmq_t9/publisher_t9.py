import json
import aio_pika

class RabbitmqPublisherTask9:

    def __init__(self, exchange):
        self.exchange = exchange

    async def publish(self, message, routing_key):
        await self.exchange.publish(
            aio_pika.Message(json.dumps(message).encode("utf-8")),
            routing_key=routing_key
        )
        print(f"Message task9 published to {routing_key} .")