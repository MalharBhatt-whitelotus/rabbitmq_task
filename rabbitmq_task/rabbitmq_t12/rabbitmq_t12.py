import aio_pika

class RabbitmqConnectionTask12:

    def __init__(self, url):
        self.url = url
        self.connection = None
        self.channel = None
        self.exchange = None
        self.retry_exchange = None
        self.retry_queue = None
        self.dlq_exchange = None
        self.dlq = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
            name="main_exchange",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
            )
        print("Main exchange initialized...")
        self.retry_exchange = await self.channel.declare_exchange(
            name="retry_exchange",
            type=aio_pika.ExchangeType.DIRECT, 
            durable=True,
        )
        print("Retry exchange initialized...")
        self.retry_queue = await self.channel.declare_queue(
            name="retry_queue", 
            durable=True,
            arguments={
                "x-message-ttl": 5000,
                "x-dead-letter-exchange": "main_exchange",
                "x-dead-letter-routing-key": "file.uploaded",

            }
        )
        print("Retry Queue initialized...")
        await self.retry_queue.bind(exchange=self.retry_exchange, routing_key="retry",)
        print("Retry Queue binded...")

        self.dlq_exchange = await self.channel.declare_exchange(
            name="dlq_exchange",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True
        )
        print("DLQ exchange initialized...")
        self.dlq = await self.channel.declare_queue(name="dlq", durable=True)
        print("DLQ initialized...")
        await self.dlq.bind(exchange=self.dlq_exchange, routing_key="failed")
        print("DLQ binded...")
        print("Rabbitmq task12 connected...")

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task12 closed...")