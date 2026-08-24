import asyncio

from rabbitmq_t11 import RabbitmqConnectionTask11
from publisher_t11 import RabbitmqPublisherTask11

async def main11():
    rabbitmq = RabbitmqConnectionTask11("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    await rabbitmq.channel.declare_queue(name="file_queue", durable=True)
    publisher = RabbitmqPublisherTask11(rabbitmq.exchange)
    await publisher.publish({"event": "file.uploaded", "file_id": 1, "retry_count":0})
    await asyncio.sleep(2)
    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main11())