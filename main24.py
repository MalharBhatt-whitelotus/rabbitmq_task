import asyncio
from datetime import datetime, timezone

from rabbitmq_t24 import RabbitmqConnectionTask24
from publisher_t24 import RabbitmqPublisherTask24

async def main24() -> None:
    rabbitmq = RabbitmqConnectionTask24("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask24(rabbitmq.main_exchange)
    for i in range(1, 11):
        print(f"Publishing message {i}...")
        await publisher.publish(
            message={
                "body":{
                    "file_id": i,
                    "document": "document.pdf",
                },
                "headers":{
                    "retry_count": 0,
                    "message_id": i,
                    "timestamp": datetime.now(timezone.utc),
                },
            },
            routing_key="main_task24.key",
        )
    await asyncio.sleep(2)
    await rabbitmq.close()


if __name__ == "__main__":
    asyncio.run(main24())