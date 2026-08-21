import asyncio
from consumer_t27 import RabbitmqConsumerTask27

if __name__ == "__main__":
    asyncio.run(
        RabbitmqConsumerTask27("amqp://guest:guest@localhost:5672").main()
    )