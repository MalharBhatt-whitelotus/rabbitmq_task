import asyncio

from rabbitmq_t16 import RabbitmqConnectionTask16
from publisher_t16 import RabbitmqPublisherTask16

async def main16():
    rabbitmq = RabbitmqConnectionTask16("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask16(rabbitmq.exchange)
    await publisher.publish(
        {
            "event": "task16.key",
            "message_id": 4542,
        }
    )
    await asyncio.sleep(2)
    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main16())