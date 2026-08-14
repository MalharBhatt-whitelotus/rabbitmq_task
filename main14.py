import asyncio

from rabbitmq_t14 import RabbitmqConnectionTask14
from publisher_t14 import RabbitmqPublisherTask14

async def main14():
    rabbitmq = RabbitmqConnectionTask14("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask14(rabbitmq.exchange)
    for i in range(1,21):
        await publisher.publish(
            {
                "event": "competitor.consumer",
                "file_id": i,
            }
        )
        print(f"Published: Message-{i}")
    await asyncio.sleep(2)
    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main14())