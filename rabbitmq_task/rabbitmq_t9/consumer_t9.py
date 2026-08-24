import json
import asyncio
from aio_pika.abc import AbstractIncomingMessage

from rabbitmq_t9 import RabbitmqConnectionTask9

class RabbitmqConsumerTask9:
    def __init__(self,routing_key: str):
        self.routing_key = routing_key

    async def consume(self, message: AbstractIncomingMessage):
        try:
            message_body = json.loads(message.body.decode())
            print(f"Message Received from {self.routing_key}..")
            print(message_body)
            await message.ack()
            print("Message Acknowledged")
        except Exception:
            raise

    async def main(self):
        rabbitmq = RabbitmqConnectionTask9("amqp://guest:guest@localhost:5672")
        await rabbitmq.connect()
        queue = await rabbitmq.channel.declare_queue(name="file_queue", durable=True)
        await queue.bind(exchange=rabbitmq.exchange, routing_key=self.routing_key)
        await queue.consume(self.consume, no_ack=False)
        await asyncio.Future()