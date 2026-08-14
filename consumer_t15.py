import asyncio
import json

import aio_pika
from aio_pika import IncomingMessage


class RabbitMQConsumer:

    def __init__(self, rabbitmq, queue):
        self.rabbitmq = rabbitmq
        self.queue = queue
        self.consumer_tag = None

    async def start(self):
        self.consumer_tag = await self.queue.consume(
            self.consume
        )

        print("Consumer started")

    async def consume(self, message: IncomingMessage):

        try:
            body = json.loads(message.body.decode())

            print(f"Processing: {body}")

            # Simulate work
            await asyncio.sleep(5)

            await message.ack()

            print(f"Completed: {body}")

        except Exception as e:
            print(f"Processing failed: {e}")
            await message.nack(requeue=True)

    async def stop(self):

        print("Stopping consumer...")

        if self.queue and self.consumer_tag:
            await self.queue.cancel(self.consumer_tag)
            print("Stopped consuming new messages")