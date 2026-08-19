import json
import aio_pika

class RabbitmqPublisherTask20:


    def __init__(self, exchange):
        self.exchange = exchange


    async def publish(self, message, routing_key: str) -> None:
        await self.exchange.publish(
            aio_pika.Message(json.dumps(message).encode()),
            routing_key=routing_key,   
        )
        print(f"Message {message} is published to {routing_key}...")