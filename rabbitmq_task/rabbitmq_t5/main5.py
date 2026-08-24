import asyncio
from random import randint

from rabbitmq_t5 import RabbitmqConnectionTask5
from publisher_t5 import RabbitmqPublisherTask5

async def main5():

    rabbitmq = RabbitmqConnectionTask5("amqp://guest:guest@localhost:5672")

    await rabbitmq.connect()

    await rabbitmq.channel.declare_queue(name="test_queue5", durable=True)

    publisher = RabbitmqPublisherTask5(rabbitmq.channel)

    for i in range(1,10):
        await publisher.publish({"event":"file.uploaded5", "file_id": i})

    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main5())