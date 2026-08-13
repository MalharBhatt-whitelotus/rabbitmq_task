import asyncio

from rabbitmq_t7 import RabbitmqConnectionTask7
from publisher_t7 import RabbitmqPublisherTask7

async def main7():

    rabbitmq = RabbitmqConnectionTask7("amqp://guest:guest@localhost:5672")

    await rabbitmq.connect()

    await rabbitmq.channel.declare_queue(name="test_queue7", durable=True)

    publisher = RabbitmqPublisherTask7(rabbitmq.channel)

    for i in range(1,11):
        await publisher.publish({"event": "file.uploaded7", "file_id": i})

    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main7())