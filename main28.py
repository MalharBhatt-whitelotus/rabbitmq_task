import asyncio

from rabbitmq_t28 import RabbitmqConnectionTask28
from publisher_t28 import RabbitmqPublisherTask28


async def main():
    rabbitmq = RabbitmqConnectionTask28("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask28(rabbitmq.main_exchange)
    for i in range(1, 101):
        await publisher.publisher(
            message={
                "event":i
            }
        )
    await asyncio.sleep(1)
    await rabbitmq.close()


if __name__ == "__main__":
    asyncio.run(main())