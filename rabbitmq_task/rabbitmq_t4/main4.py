import asyncio

from rabbitmq_t4 import RabbitmqConnectionTask4
from publisher_t4 import RabbitmqPublisherTask4

async def main4():

    rabbitmq = RabbitmqConnectionTask4("amqp://guest:guest@localhost:5672")

    await rabbitmq.connect()

    await rabbitmq.channel.declare_queue(name="test_queue4", durable=True)

    publisher = RabbitmqPublisherTask4(rabbitmq.channel)

    for i in range(1,11):
        await publisher.publish(
            {
                "event": "file.uploaded4",
                "file_id": i,
            }
        )
    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main4())