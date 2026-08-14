import json
import aio_pika


class RabbitmqPublisherTask12:
    def __init__(self, exchange):
        self.exchange = exchange

    async def publish(self,message):
        await self.exchange.publish(
            aio_pika.Message(body=json.dumps(message).encode()),
            routing_key="file.uploaded",
        )
        print("Message task12 Published...")