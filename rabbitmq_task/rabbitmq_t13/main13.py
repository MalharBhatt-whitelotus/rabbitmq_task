import asyncio

from rabbitmq_t13 import RabbitmqConnectionTask13
from publisher_t13 import RabbitmqPublisherTask13

async def main13():
    rabbitmq = RabbitmqConnectionTask13("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask13(rabbitmq.exchange)
    for i in range(1,21):
        await publisher.publish({
            "event": "consumer.uploaded",
            "file_id":i,
        })
        print(f"Published Message-{i}")
    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main13())