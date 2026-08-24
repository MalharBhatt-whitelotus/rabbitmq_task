import asyncio

from rabbitmq_t22 import RabbitmqConnectionTask22
from publisher_t22 import RabbitmqPublisherTask22

async def main22():
    rabbitmq = RabbitmqConnectionTask22("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask22(rabbitmq.main_exchange)
    for i in range(1,11):
        print(f"Message {i} publishing...")
        await publisher.publish(
            message={
                "message_id": i,
                "event": "Retry attempt delay",
                "retry_count": 0,
            },
            routing_key="task22.key",
        )

if __name__ == "__main__":
    asyncio.run(main22())