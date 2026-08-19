import aio_pika

class RabbitmqConnectionTask18:

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
        self.message_ids = set()

    async def connect(self) -> None:
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel(publisher_confirms=True)
        #--------------------
        #    Main Exchange
        #--------------------
        self.exchange = await self.channel.declare_exchange(
            name="task18_exchange",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.queue = await self.channel.declare_queue(
            name="task18_queue",
            durable=True
        )
        await self.queue.bind(
            exchange=self.exchange,
            routing_key="task18.key",
        )
        print("Main queue connected...")
        #--------------------
        #    Retry Exchange
        #--------------------
        self.retry_exchange = await self.channel.declare_exchange(
            name="retry_exchange_task18",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.retry_queue = await self.channel.declare_queue(
            name="retry_queue_task18",
            durable=True,
            arguments={
                "x-message-ttl": 5000,
                "x-dead-letter-exchange": "task18_exchange",
                "x-dead-letter-routing-key": "task18.key",
            },
        )
        await self.retry_queue.bind(
            exchange=self.retry_exchange,
            routing_key="retry.task18",
        )
        print("Retry queue connected...")
        #--------------------
        #    DLQ Exchange
        #--------------------
        self.dlq_exchange = await self.channel.declare_exchange(
            name="dlq_exchange_task18",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.dlq = await self.channel.declare_queue(
            name="dlq_task18",
            durable=True,
            arguments={
                "x-message-ttl": 10000,
            }
        )
        await self.dlq.bind(
            exchange=self.dlq_exchange,
            routing_key="dlq.task18",
        )
        print("DLQ connected...")
        print("Rabbitmq Task18 connected...")

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task18 closed...")