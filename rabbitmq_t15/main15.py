import asyncio

from rabbitmq_t15 import RabbitmqConnectionTask15
from publisher_t15 import RabbitmqPublisherTask15

async def main15():
    rabbitmq = RabbitmqConnectionTask15("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask15(rabbitmq.exchange)
    for i in range(1, 21):
        await publisher.publish({
            "event": "graceful shutdown",
            "file_id": i
            })
        print(f"Published: Message-{i}")
    await asyncio.sleep(2)
    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main15())