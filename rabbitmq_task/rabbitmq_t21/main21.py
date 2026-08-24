import asyncio

from rabbitmq_t21 import RabbitmqConnectionTask21
from publisher_t21 import RabbitmqPublisherTask21

async def main21():
    rabbitmq = RabbitmqConnectionTask21("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask21(rabbitmq.exchange)
    await publisher.publish(
        message={"event": "messaging."}
    )
    await asyncio.sleep(2)
    await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main21())