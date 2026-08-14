import aio_pika

class RabbitmqConnectionTask15:

    def __init__(self, url):
        self.url = url
        self.connection = None
        self.channel = None
        self.exchange = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(name="grace_shut", type=aio_pika.ExchangeType.DIRECT, durable=False)
        self.queue = await self.channel.declare_queue(name="grace_shut_queue", durable=True)
        await self.queue.bind(exchange=self.exchange, routing_key="grace_shut")
        print("Rabbitmq task15 connected...")

    async def close(self):
        if self.connection:
            await self.connection.close()