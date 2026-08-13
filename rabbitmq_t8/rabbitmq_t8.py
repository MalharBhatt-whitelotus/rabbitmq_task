import aio_pika

class RabbitmqConnectionTask8:

    def __init__(self, url: str) -> None:
        self.url = url
        self.connection = None
        self.channel = None
        self.exchange = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
            name="file_exchange",
            type= aio_pika.ExchangeType.DIRECT,
            durable=True
            )
        print("Rabbitmq task8 connected...")

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task 8 connection closed.")