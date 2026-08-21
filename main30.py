import asyncio

from rabbitmq_t30 import RabbitmqConnectionTask30
from publisher_t30 import RabbitmqPublisherTask30


async def main() -> None:
    rabbitmq = RabbitmqConnectionTask30("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask30(rabbitmq.file_upload_exchange)
    for i in range(1, 11):
        print(f"Message:{i} Publishing...")
        await publisher.publish(
            message={
                "body":{
                    "file_id": i,
                    "document": f"file{i}.file",
                },
                "headers":{
                    "retry_count": 0,
                    "event": "file_upload",
                },
            },
            routing_key="file_upload_task30.key"
        )


if __name__ == "__main__":
    asyncio.run(main())