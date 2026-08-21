import aio_pika


class RabbitmqConnectionTask27:


    def __init__(self, url: str) -> None:
        self.url = url
        self.connection = None
        self.channel = None
        self.main_exchange = None
        self.main_queue = None
        self.retry_exchange = None
        self.retry_queues = {}
        self.dlq_exchange = None
        self.dlq  = None
        self.message_ids = set()
        self.attempts = 3


    async def connect(self) -> None:
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel(publisher_confirms=True)
        await self.channel.set_qos(prefetch_count=1)
        self.main_exchange = await self.channel.declare_exchange(
            name="main_exchange_task27",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.main_queue = await self.channel.declare_queue(
            name="main_queue_task27",
            durable=True,
        )
        await self.main_queue.bind(
            exchange=self.main_exchange,
            routing_key="main_task27.key"
        )
        self.retry_exchange = await self.channel.declare_exchange(
            name="retry_exchange_task27",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        for i in range(self.attempts):
            queue = await self.channel.declare_queue(
                name=f"retry_{i}_queue_task27",
                durable=True,
                arguments={
                    "x-message-ttl": 5000 * (2 ** i),
                    "x-dead-letter-exchange": "main_exchange_task27",
                    "x-dead-letter-routing-key": "main_task27.key",
                }
            )
            await queue.bind(
                exchange=self.retry_exchange,
                routing_key=f"retry_{i}_task27.key"
            )
            self.retry_queues[i] = queue
        self.dlq_exchange = await self.channel.declare_exchange(
            name="dlq_exchange_task27",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.dlq = await self.channel.declare_queue(
            name="dlq_task27",
            durable=True,
        )
        await self.dlq.bind(
            exchange=self.dlq_exchange,
            routing_key="dlq_task27.key"
        )


    async def close(self):
        if self.connection:
            await self.connection.close()