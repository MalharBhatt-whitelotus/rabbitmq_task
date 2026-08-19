import json
import aio_pika

class RabbitmqPublisherTask18:

    def __init__(self, exchange):
        self.exchange = exchange

    async def publish(self, message):
        await self.exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(), 
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key="task18.key",
        )
        print("Message task18 published....")