import aio_pika

class RabbitmqConnectionTask17:

    def __init__(self, url):
        self.url = url
        self.connection = None
        self.channel = None
        self.exchange = None
        self.queue = None
        self.messages_ids = set()

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel(publisher_confirms=True)
        self.exchange = await self.channel.declare_exchange(name="task17_exchange", type=aio_pika.ExchangeType.DIRECT, durable=True)
        self.queue = await self.channel.declare_queue(name="task17_queue", durable=True)
        await self.queue.bind(exchange=self.exchange, routing_key="task17.key")
        print("Rabbitmq task17 connected...")

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task17 closed....")