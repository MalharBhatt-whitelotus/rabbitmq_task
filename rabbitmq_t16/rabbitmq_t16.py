import aio_pika

class RabbitmqConnectionTask16:

    def __init__(self, url):
        self.url = url
        self.connection = None
        self.channel = None
        self.exchange = None
        self.queue = None
        self.messages_ids = set()

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(name="task16_exchange", type=aio_pika.ExchangeType.DIRECT, durable=True)
        self.queue = await self.channel.declare_queue(name="task16_queue", durable=True)
        await self.queue.bind(exchange="task16_exchange", routing_key="task16.key",)
        print("Rabbitmq task16 connection successful...")

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task16 connection closed...")