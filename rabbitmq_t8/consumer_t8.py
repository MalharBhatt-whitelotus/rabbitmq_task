import json
import asyncio
from aio_pika.abc import AbstractIncomingMessage

from rabbitmq_t8 import RabbitmqConnectionTask8

async def consume(message: AbstractIncomingMessage):
    try:
        message_body = json.loads(message.body.decode())
        if message_body["file_id"] == 6:
            raise Exception("Error with 6...")
        print("Message task 8 received.")
        print(message_body)
        await message.ack()
        print("Message acknowledged")
    except Exception:
        print("Message not acknowledged.")
        raise

async def main():
    rabbitmq = RabbitmqConnectionTask8("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    queue = await rabbitmq.channel.declare_queue(name="file_queue", durable=True)
    await queue.bind(exchange=rabbitmq.exchange, routing_key="file.uploaded")
    await queue.consume(consume, no_ack=False)

    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())