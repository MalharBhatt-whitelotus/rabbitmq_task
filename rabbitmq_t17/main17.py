import asyncio

from rabbitmq_t17 import RabbitmqConnectionTask17
from publisher_t17 import RabbitmqPublisherTask17

async def main17():
    rabbitmq = RabbitmqConnectionTask17("amqp://guest:guest@localhost:5672")
    await rabbitmq.connect()
    publisher = RabbitmqPublisherTask17(rabbitmq.exchange)
    message = {
        "event": "task17.key",
        "message_id": 1
    }
    try:
        # await rabbitmq.exchange.delete() #!To trigger the exception...
        await publisher.publish(message)
        print("Message confirmed by RabbitMQ")
    except Exception as exc:
        print(f"Message was not confirmed: {exc}")
    finally:
        await asyncio.sleep(2)
        await rabbitmq.close()

if __name__ == "__main__":
    asyncio.run(main17())