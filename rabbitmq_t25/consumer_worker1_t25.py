import asyncio
from consumer_t25 import RabbitmqConsumerTask25

if __name__ == "__main__":
    asyncio.run(
        RabbitmqConsumerTask25(url="amqp://guest:guest@localhost:5672").main()
    )