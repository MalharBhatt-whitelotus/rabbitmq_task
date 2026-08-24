import asyncio

from rabbitmq_t27 import RabbitmqConnectionTask27
from publisher_t27 import RabbitmqPublisherTask27


async def main27():
    rabbitmq = RabbitmqConnectionTask27("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask27(rabbitmq.main_exchange)
    for i in range(1, 11):
        await publisher.publish(
            message={
                "body":{
                    "file_id": i
                },
                "headers":{
                    "retry_count": 0,
                    "message_id": i
                }
            },
            routing_key="main_task27.key"
        )
    await asyncio.sleep(2)
    await rabbitmq.close()


if __name__ == "__main__":
    asyncio.run(main27())