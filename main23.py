import asyncio
from datetime import datetime, timezone

from rabbitmq_t23 import RabbitmqConnectionTask23
from publisher_t23 import RabbitmqPublisherTask23

async def main23():
    rabbitmq = RabbitmqConnectionTask23("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask23(rabbitmq.main_exchange)
    for i in range(1, 11):
        print(f"Publishing message ID: {i}")
        timestamp = datetime.now(timezone.utc)
        message = {
            "body": {
                "file_id": i,
                "event": "file_upload",
            },
            "headers":{
                "retry_count": 0,
                "message_id": i,
                "timestamp": timestamp
            }
        }
        await publisher.publish(
            message=message,
            routing_key="main_task23.key",
        )


if __name__ == "__main__":
    asyncio.run(main23())