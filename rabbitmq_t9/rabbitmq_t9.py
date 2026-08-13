import aio_pika

class RabbitmqConnectionTask9:

    def __init__(self, url,):
        self.url = url
        self.connection = None
        self.channel = None
        self.exchange = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(name="file_exchange", type=aio_pika.ExchangeType.DIRECT, durable=True)
        print("Rabbitmq task9 connected...")

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task9 closed...")