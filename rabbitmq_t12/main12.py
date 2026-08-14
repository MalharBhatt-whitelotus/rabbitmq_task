import asyncio

from rabbitmq_t12 import RabbitmqConnectionTask12
from publisher_t12 import RabbitmqPublisherTask12

async def main12():
    rabbitmq = RabbitmqConnectionTask12("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    await rabbitmq.channel.declare_queue("main_queue", durable=True)

    publisher = RabbitmqPublisherTask12(rabbitmq.exchange)
    for i in range(1, 111):
        await publisher.publish(
            {
                "event": "file.uploaded",
                "file_id": i,
                "retry_count":0,
            }
        )
    await asyncio.sleep(2)
    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main12())