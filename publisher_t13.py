import aio_pika
import json

class RabbitmqPublisherTask13:

    def __init__(self, exchange):
        self.exchange = exchange

    async def publish(self, message):
        await self.exchange.publish(
            aio_pika.Message(body=json.dumps(message).encode()),
            routing_key="consumer.uploaded",
        )
        print("Message task13 publish ...")