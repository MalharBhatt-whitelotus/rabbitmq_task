import asyncio
from aio_pika.abc import AbstractIncomingMessage
from random import randint


from rabbitmq_t5 import RabbitmqConnectionTask5
from publisher_t5 import RabbitmqPublisherTask5

async def consume(message: AbstractIncomingMessage):
    async with message.process():
        print("Processing..")
        random = randint(1,10)
        await asyncio.sleep(random)
        print("Message task5 received.")
        print(message.body.decode())

async def main():

    rabbitmq = RabbitmqConnectionTask5("amqp://guest:guest@localhost:5672")

    await rabbitmq.connect()

    queue = await rabbitmq.channel.declare_queue(name="test_queue5", durable=True)

    await queue.consume(consume)

    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())