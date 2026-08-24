import json
import aio_pika

class RabbitmqPublisherTask14:

    def __init__(self, exchange):
        self.exchange = exchange

    async def publish(self, message):
        await self.exchange.publish(
            aio_pika.Message(json.dumps(message).encode()),
            routing_key="competitor.consumer"
        )
        print("Message task14 published....")