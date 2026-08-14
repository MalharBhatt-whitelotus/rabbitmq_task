import asyncio
import signal

from rabbitmq_t15 import RabbitmqConnectionTask15
from consumer_t15 import RabbitMQConsumer


async def main():

    rabbitmq = RabbitmqConnectionTask15(
        "amqp://guest:guest@localhost:5672"
    )

    await rabbitmq.connect()

    consumer = RabbitMQConsumer(rabbitmq, rabbitmq.queue)

    await consumer.start()

    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):

        loop.add_signal_handler(
            sig,
            stop_event.set
        )

    print("Application running...")
    print("Press CTRL+C to shutdown")

    try:

        await stop_event.wait()

    finally:

        print("Shutdown signal received")

        await consumer.stop()

        print("Closing channel...")
        await rabbitmq.channel.close()

        print("Closing connection...")
        await rabbitmq.connection.close()

        print("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())