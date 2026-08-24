import asyncio

from rabbitmq_t2 import RabbitmqConnectionTask2
from publisher_t2 import RabbitmqPublisherTask2

async def consume(message: dict):
        async with message.process():
            print("Message Received.")
            print(message.body.decode())


async def main():

    rabbitmq = RabbitmqConnectionTask2("amqp://guest:guest@localhost:5672/")

    await rabbitmq.connect()

    queue = await rabbitmq.channel.declare_queue(name="test_queue2", durable=True)

    await queue.consume(consume)

    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())