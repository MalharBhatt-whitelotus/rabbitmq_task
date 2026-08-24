import asyncio

from rabbitmq_t1 import RabbitmqConnectionTask1

async def consume(message):
    async with message.process():
        print("Message Received.")
        print(message.body.decode())

async def main():

    rabbitmq = RabbitmqConnectionTask1(url= "amqp://guest:guest@localhost:5672/")

    await rabbitmq.connect()

    queue = await rabbitmq.channel.declare_queue(name="test_queue1", durable=True)

    await queue.consume(consume)

    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main=main())