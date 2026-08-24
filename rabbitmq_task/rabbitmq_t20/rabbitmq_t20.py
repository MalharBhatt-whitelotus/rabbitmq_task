import aio_pika

class RabbitmqConnectionTask20:


    def __init__(self, url):
        self.url = url
        self.connection = None
        self.channel = None
        self.main_exchange = None
        self.main_queue = None
        self.retry_exchange = None
        self.retry_queue = None
        self.dlq_exchange = None
        self.dlq = None
        self.message_ids = set()


    async def connect(self):
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel(publisher_confirms=True)
        await self.channel.set_qos(prefetch_count=1)
        #------------------
        #  Main Exchange
        #------------------
        self.main_exchange = await self.channel.declare_exchange(
            name="main_exchange_task20",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.main_queue = await self.channel.declare_queue(
            name="main_queue_task20",
            durable=True,
        )
        await self.main_queue.bind(
            exchange=self.main_exchange,
            routing_key="main_task20.key",
        )
        print("Main queue is connected...")
        #------------------
        #  Retry Exchange
        #------------------
        self.retry_exchange = await self.channel.declare_exchange(
            name="retry_exchange_task20",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.retry_queue = await self.channel.declare_queue(
            name="retry_queue_task20",
            durable=True,
            arguments={
                "x-message-ttl": 5000,
                "x-dead-letter-exchange": "main_exchange_task20",
                "x-dead-letter-routing-key": "main_task20.key",
            },
        )
        await self.retry_queue.bind(
            exchange=self.retry_exchange,
            routing_key="retry.task20",
        )
        print("Retry queue is connected...")
        #------------------
        #  DLQ Exchange
        #------------------
        self.dlq_exchange = await self.channel.declare_exchange(
            name="dlq_exchange_task20",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.dlq = await self.channel.declare_queue(
            name="dlq_task20",
            durable=True,
        )
        await self.dlq.bind(
            exchange=self.dlq_exchange,
            routing_key="dlq_task20.key",
        )
        print("DLQ is connected...")
        print("Rabbitmq task20 is connected....")


    async def close(self):
        if self.connection:
            await self.connection.close()
            print("Rabbitmq task20 is closed...")