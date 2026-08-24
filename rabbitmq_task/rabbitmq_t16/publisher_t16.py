import json
import aio_pika

class RabbitmqPublisherTask16:

    def __init__(self, exchange):
        self.exchange = exchange

    async def publish(self, message):
        await self.exchange.publish(
            aio_pika.Message(body=json.dumps(message).encode()),
            routing_key="task16.key"
        )
        print("Message task16 published....")