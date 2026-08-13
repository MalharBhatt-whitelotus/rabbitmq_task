import asyncio

from rabbitmq_t3 import RabbitmqConnectionTask3

async def consume(message: dict):
    async with message.process():
        print(message.body.decode())
        print("Message received.")

async def main():

    rabbitmq = RabbitmqConnectionTask3("amqp://guest:guest@localhost:5672/")

    await rabbitmq.connect()

    queue = await rabbitmq.channel.declare_queue(name="test_queue3", durable=True)

    await queue.consume(consume)

    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())