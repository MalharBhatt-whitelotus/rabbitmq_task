import asyncio

from rabbitmq_t26 import RabbitmqConnectionTask26
from publisher_t26 import RabbitmqPublisherTask26

async def main26():
    rabbitmq = RabbitmqConnectionTask26("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask26(rabbitmq.exchange)
    await publisher.publish(
        message={
            "corrupt": True,
            "retry_count": 0,
        }
    )
    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main26())