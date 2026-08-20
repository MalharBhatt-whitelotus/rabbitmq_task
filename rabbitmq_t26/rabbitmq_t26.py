import aio_pika


class RabbitmqConnectionTask26:


    def __init__(self, url: str) -> None:
        self.url = url
        self.connection = None
        self.channel = None
        self.exchange = None
        self.queue = None
        self.retry_exchange = None
        self.retry_queue = None
        self.dlq_exchange = None
        self.dlq = None


    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        self.exchange = await self.channel.declare_exchange(
            name="main_exchange_task26",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True
        )
        self.queue = await self.channel.declare_queue(
            name="main_queue_task26",
            durable=True
        )
        await self.queue.bind(
            exchange=self.exchange,
            routing_key="main_task26.key"
        )
        self.retry_exchange = await self.channel.declare_exchange(
            name="retry_exchange_task26",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True
        )
        self.retry_queue = await self.channel.declare_queue(
            name="retry_queue_task26",
            durable=True,
            arguments={
                "x-message-ttl": 2000,
                "x-dead-letter-exchange": "main_exchange_task26",
                "x-dead-letter-routing-key": "main_task26.key",
            }
        )
        await self.retry_queue.bind(
            exchange=self.retry_exchange,
            routing_key="retry_task26.key",
        )
        self.dlq_exchange = await self.channel.declare_exchange(
            name="dlq_exchange_task26",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.dlq = await self.channel.declare_queue(
            name="dlq_task26",
            durable=True
        )
        await self.dlq.bind(
            exchange=self.dlq_exchange,
            routing_key="dlq_task26.key"
        )
        print("Rabbitmq task26 is connected...")


    async def close(self):
        if self.connection:
            await self.connection.close()
            print("rabbitmq task26 closed...")