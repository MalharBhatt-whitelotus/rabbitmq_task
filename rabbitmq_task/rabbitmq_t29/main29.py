import asyncio

from rabbitmq_t29 import RabbitmqConnectionTask29
from publisher_t29 import RabbitmqPublisherTask29


async def main() -> None:
    rabbitmq = RabbitmqConnectionTask29("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask29(rabbitmq.main_exchange)
    for i in range(1, 111):
        await publisher.publish(
            message={
                "event": i,
            }
        )
    await asyncio.sleep(2)
    await rabbitmq.close()


if __name__ == "__main__":
    asyncio.run(main())