import asyncio

from rabbitmq_t4 import RabbitmqConnectionTask4

async def consume(message: dict):
    async with message.process():
        print(message.body.decode())
        print("Message task 4 received.")

async def main():

    rabbitmq = RabbitmqConnectionTask4("amqp://guest:guest@localhost:5672/")

    await rabbitmq.connect()

    queue = await rabbitmq.channel.declare_queue(name="test_queue4", durable=True)

    await queue.consume(consume)

    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())