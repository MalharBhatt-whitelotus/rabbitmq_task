import asyncio

from consumer_t22 import RabbitmqConsumerTask22

if __name__ == "__main__":
    asyncio.run(RabbitmqConsumerTask22(url="amqp://guest:guest@localhost:5672", consumer_num=1).main())