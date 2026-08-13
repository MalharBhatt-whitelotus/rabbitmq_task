import json
import asyncio
from aio_pika.abc import AbstractIncomingMessage

from rabbitmq_t7 import RabbitmqConnectionTask7

async def consume(message: AbstractIncomingMessage):
    try:
        message_body = json.loads(message.body.decode())
        if message_body["file_id"] == 6:
            print("Message not acknowledged...")
            raise Exception("Intentional raising exception for fileid 6.")
        print("Message received.")
        print(message_body)
        await message.ack()
        print("Message Acknowledged..")

    except Exception:
        print("Message not acknowledged...")
        raise

async def main():
    rabbitmq  = RabbitmqConnectionTask7("amqp://guest:guest@localhost:5672")

    await rabbitmq.connect()

    queue = await rabbitmq.channel.declare_queue(name="test_queue7", durable=True)

    await queue.consume(consume, no_ack=False)

    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())