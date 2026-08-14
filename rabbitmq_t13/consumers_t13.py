import asyncio

from rabbitmq_t13 import RabbitmqConnectionTask13
from consumer_t13 import consume

async def consumer_worker(name):
    rabbitmq = RabbitmqConnectionTask13("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    await rabbitmq.queue.consume(
        lambda message : consume(message, name)
    )
    print(f"{name} started....")

    await asyncio.Future()

async def main():
    await asyncio.gather(
        consumer_worker("consumer-1"),
        consumer_worker("consumer-2")
    )

if __name__ == "__main__":
    asyncio.run(main())