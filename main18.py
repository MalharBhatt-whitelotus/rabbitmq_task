import json
import asyncio

from rabbitmq_t18 import RabbitmqConnectionTask18
from publisher_t18 import RabbitmqPublisherTask18

async def main18():
    rabbitmq = RabbitmqConnectionTask18("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask18(rabbitmq.exchange)
    try:
        for i in range(1,11):
            await publisher.publish(
                {
                    "event": "task18.key",
                    "message_id": i,
                    "retry_count": 0
                },
            )
            print(f"Message {i} is publishing...")
            await asyncio.sleep(2)
    except Exception as exc:
        print(f"Something went wrong... {exc}")
        raise
    finally:
        await asyncio.sleep(2)
        await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main18())