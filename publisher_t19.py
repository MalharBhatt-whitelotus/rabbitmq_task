import json
import aio_pika

class RabbitmqPublisherTask19:

    def __init__(self, exchange) -> None:
        self.exchange = exchange

    async def publish(self, message, routing_key):
        await self.exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=routing_key,
        )
        print(f"Published message {message} to routing key {routing_key}...")