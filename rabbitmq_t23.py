import aio_pika

class RabbitmqConnectionTask23:


    def __init__(self, url: str) -> None:
        self.url = url
        self.connection = None
        self.channel = None
        self.main_exchange = None
        self.main_queue = None
        self.retry_exchange = None
        self.retry_queues = {}
        self.dlq_exchange = None
        self.dlq = None
        self.message_ids = set()
        self.attempts = 3


    async def connect(self) -> None:
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel(publisher_confirms=True, on_return_raises=True)
        await self.channel.set_qos(prefetch_count=1)
        #--------------------
        #   Main Exchange
        #--------------------
        self.main_exchange = await self.channel.declare_exchange(
            name="main_exchange_task23",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.main_queue = await self.channel.declare_queue(
            name="main_queue_task23",
            durable=True,
        )
        await self.main_queue.bind(
            exchange=self.main_exchange,
            routing_key="main_task23.key",
        )
        print("Main queue is created...")
        #--------------------
        #   Retry Exchange
        #--------------------
        self.retry_exchange = await self.channel.declare_exchange(
            name="retry_exchange_task23",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        for i in range(0, self.attempts):
            self.retry_queue = await self.channel.declare_queue(
                name=f"retry_{i}_queue_task23",
                durable=True,
                arguments={
                    "x-message-ttl": 5000 * (2 ** i),
                    "x-dead-letter-exchange": "main_exchange_task23",
                    "x-dead-letter-routing-key": "main_task23.key",
                }
            )
            await self.retry_queue.bind(
                exchange=self.retry_exchange,
                routing_key=f"retry_{i}_task23.key",
            )
            print(f"Retry {i} queue created...")
        #--------------------
        #   DLQ Exchange
        #--------------------
        self.dlq_exchange = await self.channel.declare_exchange(
            name="dlq_exchange_task23",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.dlq = await self.channel.declare_queue(
            name="dlq_task23",
            durable=True,
        )
        await self.dlq.bind(
            exchange=self.dlq_exchange,
            routing_key="dlq_task23.key",
        )
        print("DLQ created...")
        print("Rabbitmq task23 connection is created...")


    async def close(self) -> None:
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task23 connection is closed...")