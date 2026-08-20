import asyncio
from consumer_t23 import RabbitmqConsumerTask23

if __name__ == "__main__":
    asyncio.run(
        RabbitmqConsumerTask23(
            url="amqp://guest:guest@localhost:5672",
            consumer_number=1,
        ).main()
    )