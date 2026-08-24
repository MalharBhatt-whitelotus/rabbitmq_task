import asyncio

from consumer_t24 import RabbitmqConsumerTask24

if __name__ == "__main__":
    asyncio.run(
        RabbitmqConsumerTask24(
            url="amqp://guest:guest@localhost:5672",
            con_num=2
        ).main()
    )