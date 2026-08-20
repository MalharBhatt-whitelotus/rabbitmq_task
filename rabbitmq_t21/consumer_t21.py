import json
import asyncio
from aio_pika import IncomingMessage

from rabbitmq_t21 import RabbitmqConnectionTask21

async def consume(message: IncomingMessage) -> None:
    print("message is received...")
    print(json.loads(message.body.decode()))
    await message.ack()
    print("Message is acknowledged")

async def main():
    rabbitmq = RabbitmqConnectionTask21("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    await rabbitmq.queue.consume(consume)
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
