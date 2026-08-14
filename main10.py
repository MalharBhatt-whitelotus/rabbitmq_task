import asyncio

from rabbitmq_t10 import RabbitmqConnectionTask10
from publisher_t10 import RabbitmqPublisherTask10

async def main10():

    rabbitmq = RabbitmqConnectionTask10("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    await rabbitmq.channel.declare_queue(name="file_queue", durable=True)
    publisher = RabbitmqPublisherTask10(exchange=rabbitmq.exchange)
    await publisher.publish({"event": "file.uploaded", "file_id": 1, "retry_attempt":1})
    await asyncio.sleep(2)
    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main10())