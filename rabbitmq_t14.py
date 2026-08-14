import aio_pika

class RabbitmqConnectionTask14:

    def __init__(self, url):
        self.url = url 
        self.connection = None
        self.channel = None
        self.exchange = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(name="competitor_consumer", type=aio_pika.ExchangeType.DIRECT, durable=True)
        self.competitor_consumer_queue = await self.channel.declare_queue(name="competitor_consumer", durable=True)
        await self.competitor_consumer_queue.bind(exchange=self.exchange, routing_key="competitor.consumer")
        print("Rabbitmq task14 connected....")

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task14 closed...")