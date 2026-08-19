import asyncio

from rabbitmq_t20 import RabbitmqConnectionTask20
from publisher_t20 import RabbitmqPublisherTask20

async def main20():
    rabbitmq = RabbitmqConnectionTask20("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask20(rabbitmq.main_exchange)
    try:
        for i in range(1,6):
            print(f"Publishing message {i}")
            await publisher.publish(
                message={
                    "message_id": i,
                    "event" : "task 20 , main_graceful exit..",
                    "retry_count": 0,
                },
                routing_key="main_task20.key"
            )
    except Exception as exc:
        print(f"Somethine went wrong while publishing message {i}...{exc}")
        raise


if __name__ == "__main__":
    asyncio.run(main20())