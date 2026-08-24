import asyncio

from rabbitmq_t6 import RabbitmqConnectionTask6
from publisher_t6 import RabbitmqPublisherTask6

async def main6():

    rabbitmq = RabbitmqConnectionTask6("amqp://guest:guest@localhost:5672")

    await rabbitmq.connect()

    await rabbitmq.channel.declare_queue(name="test_queue6", durable=True)

    publisher = RabbitmqPublisherTask6(rabbitmq.channel)

    for i in range(1,11):
        await publisher.publish({"event": "file.uploaded6", "file_id": i})

    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main6())