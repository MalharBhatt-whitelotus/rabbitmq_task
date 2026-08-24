import json
import aio_pika

class RabbitmqPublisherTask17:

    def __init__(self, exchange):
        self.exchange = exchange

    async def publish(self, message):
        await self.exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(), 
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
            routing_key="task17.key",
        )
        print("Message task17 published...")