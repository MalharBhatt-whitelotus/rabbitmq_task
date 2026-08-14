import aio_pika

class RabbitmqConnectionTask11:

    def __init__(self, url):
        self.url = url
        self.connection = None
        self.channel = None
        self.exchange = None
        self.dlq_exchange = None
        self.dlq = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(name="file_exchange", type=aio_pika.ExchangeType.DIRECT, durable=True)
        self.dlq_exchange = await self.channel.declare_exchange(name="dlq_exchange", type=aio_pika.ExchangeType.DIRECT, durable=True)
        self.dlq = await self.channel.declare_queue(name="dlq", durable=True,)
        await self.dlq.bind(exchange=self.dlq_exchange, routing_key="failed")
        print("Rabbitmq task11 connected...")

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task11 closed...")