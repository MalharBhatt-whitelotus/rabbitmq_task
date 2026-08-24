import asyncio

from rabbitmq_t25 import RabbitmqConnectionTask25
from publisher_t25 import RabbitmqPublisherTask25


async def main25() -> None:
    rabbitmq = RabbitmqConnectionTask25("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask25(exchange=rabbitmq.main_exchange)
    for i in range(1, 61):
        await publisher.publish(
            message={
                "event": "prefetch_count"
            }
        )
    await rabbitmq.close()


if __name__ == "__main__":
    asyncio.run(main25())