import asyncio

from rabbitmq_t2 import RabbitmqConnectionTask2
from publisher_t2 import RabbitmqPublisherTask2

async def main2():

    rabbitmq = RabbitmqConnectionTask2("amqp://guest:guest@localhost:5672/")

    await rabbitmq.connect()

    await rabbitmq.channel.declare_queue(name="test_queue2", durable=True)

    publisher = RabbitmqPublisherTask2(rabbitmq.channel)

    await publisher.publish(
        {"event": "user.created", "user_id": 101},
        {"event": "file.uploaded", "file_id": 202},
        {"event": "file.deleted", "file_id": 303},
        )

    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main=main2())