import asyncio

from rabbitmq_t1 import RabbitmqConnectionTask1
from publisher_t1 import RabbitmqPublisherTask1

async def main1():

    rabbitmq = RabbitmqConnectionTask1("amqp://guest:guest@localhost:5672/")

    await rabbitmq.connect()

    await rabbitmq.channel.declare_queue(name="test_queue1", durable=True)

    publisher = RabbitmqPublisherTask1(rabbitmq.channel)

    await publisher.publish({
        "event": "file.uploaded1",
        "file_id": "1",
        "file": "document.pdf",
    })

    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main=main1())