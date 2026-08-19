import json
import asyncio

from rabbitmq_t19 import RabbitmqConnectionTask19
from publisher_t19 import RabbitmqPublisherTask19

async def main19():
    rabbitmq = RabbitmqConnectionTask19("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask19(rabbitmq.main_exchange)
    try:
        for i in range(1,11):
            print(f"Publishing Message: 'message_id: {i}' to routing key : 'main_task19.key'...")
            await asyncio.sleep(1)
            await publisher.publish(
                message={
                    "message_id": i,
                    "event": "task19: Main RABBITMQ construction.",
                    "retry_count": 0,
                },
                routing_key="main_task19.key"
            )
            await asyncio.sleep(2)
    except Exception as exc:
        print(f"Something went wrong... {exc}")
        raise

if __name__ == "__main__":
    asyncio.run(main19())