import aio_pika

class RabbitmqConnectionTask21:


    def __init__(self, url: str) -> None:
        self.url = url
        self.connection = None
        self.channel = None
        self.exchange = None
        self.queue = None


    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
            name="task21_exchange",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.queue = await self.channel.declare_queue(
            name="task21_queue",
            durable=True,
        )
        await self.queue.bind(
            exchange=self.exchange,
            routing_key="task21.key"
        )
        print("Rabbitmq task21 is connected...")


    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task21 is closed....")