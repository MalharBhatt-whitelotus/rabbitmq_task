import asyncio
from consumer_t30 import RabbitmqConsumerTask30

if __name__ == "__main__":
    asyncio.run(
        RabbitmqConsumerTask30(
            "amqp://guest:guest@localhost:5672",
            "file_process"
        ).main()
    )