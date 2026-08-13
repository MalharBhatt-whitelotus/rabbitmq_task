import asyncio
from datetime import datetime, timezone

from rabbitmq_t3 import RabbitmqConnectionTask3
from publisher_t3 import RabbimqPublisherTask3

async def main3():

    rabbitmq = RabbitmqConnectionTask3("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    await rabbitmq.channel.declare_queue(name="test_queue3", durable=True)

    publisher = RabbimqPublisherTask3(rabbitmq.channel)

    await publisher.publish({
        "event": "file.uploaded3",
        "file_id": 1,
        "file": "document.pdf",
        "timestamp": str(datetime.now(timezone.utc)),
    })

    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main3())