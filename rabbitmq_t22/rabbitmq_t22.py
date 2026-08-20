import aio_pika

class RabbitmqConnectionTask22:

    RETRY_DELAYS = {
        0: 5_000,
        1: 10_000,
        2: 20_000,
    }

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

    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel()
        self.main_exchange = await self.channel.declare_exchange(
            name="task22_exchange",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.main_queue = await self.channel.declare_queue(
            name="task22_queue",
            durable=True,
        )
        await self.main_queue.bind(
            exchange=self.main_exchange,
            routing_key="task22.key",
        )
        print("Main queue is connected...")
        self.retry_exchange = await self.channel.declare_exchange(
            name="retry_exchange_task22",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        for attempt, ttl in self.RETRY_DELAYS.items():
            queue = await self.channel.declare_queue(
                name=f"retry_{attempt}_queue_task22",
                durable=True,
                arguments={
                    "x-message-ttl": ttl,
                    "x-dead-letter-exchange": "task22_exchange",
                    "x-dead-letter-routing-key": "task22.key"
                }
            )
            routing_key= f"retry_{attempt}_task22.key"
            await queue.bind(
                exchange=self.retry_exchange,
                routing_key=routing_key
            )
            self.retry_queues[attempt] = queue
            print(
                f"Retry queue {queue} created"
                f" with routing_key={routing_key}"
                f"with TTL={ttl}ms.."
            )
        self.dlq_exchange = await self.channel.declare_exchange(
            name="dlq_exchange_task22",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.dlq = await self.channel.declare_queue(
            name="dlq_task22",
            durable=True,
        )
        await self.dlq.bind(
            exchange=self.dlq_exchange,
            routing_key="dlq_task22.key",
        )
        print("DLQ is connected...")
        print("Rabbitmq task22 is connected...")

    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task22 is closed...")