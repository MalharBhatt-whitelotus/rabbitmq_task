import asyncio

from rabbitmq_t8 import RabbitmqConnectionTask8
from publisher_t8 import RabbitmqPublisherTask8

async def main8():
    rabbitmq = RabbitmqConnectionTask8("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    await rabbitmq.channel.declare_queue(name="file_queue", durable=True)
    publisher = RabbitmqPublisherTask8(rabbitmq.exchange)
    for i in range(1,11):
        await publisher.publish({"event": "file.uploaded", "file_id": i})
    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main8())