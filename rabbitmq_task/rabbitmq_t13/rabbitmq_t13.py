import aio_pika

class RabbitmqConnectionTask13:

    def __init__(self, url):
        self.url = url
        self.connection = None
        self.channel = None
        self.exchange = None
        self.queue = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=5)
        self.exchange = await self.channel.declare_exchange(name="prefetch_exchange", type=aio_pika.ExchangeType.DIRECT, durable=True)
        self.queue = await self.channel.declare_queue(name="queue", durable=True)
        await self.queue.bind(exchange=self.exchange, routing_key="consumer.uploaded")
        print("Rabbitmq task 13 is connected..")

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task13 closed....")