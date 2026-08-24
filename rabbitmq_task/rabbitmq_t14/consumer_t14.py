import asyncio
import json
import aio_pika
from random import randint


async def consume(queue, consumer_name):

    async def callback(message: aio_pika.IncomingMessage):

        async with message.process():

            body = json.loads(message.body.decode())

            print(
                f"{consumer_name} processing "
                f"message={body['file_id']}"
            )

            # Simulate work
            time = randint(1,10)
            await asyncio.sleep(time)

            print(
                f"{consumer_name} finished "
                f"message={body['file_id']}"
                f"For time: {time}"
            )

    await queue.consume(callback)

    print(f"{consumer_name} started")