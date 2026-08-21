import aio_pika
import redis.asyncio as redis


class RabbitmqConnectionTask30:


    def __init__(self, url: str) -> None:
        self.url = url
        self.connection = None
        self.channel = None
        self.file_upload_exchange = None
        self.file_upload_queue = None
        self.file_process_exchange = None
        self.file_process_queue = None
        self.file_embedding_exchange = None
        self.file_embedding_queue = None
        self.retry_exchange = None
        self.retry_queues = {}
        self.dlq_exchange = None
        self.dlq = None
        self.message_ids = redis.Redis(
            host="localhost",
            port=6379,
            decode_responses=True,
        )
        self.attempts = 3


    async def connect(self) -> None:
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel(
            publisher_confirms=True, 
            on_return_raises=True,
        )
        await self.channel.set_qos(prefetch_count=1)
        #------------------------
        #  File Upload Exchange
        #------------------------
        self.file_upload_exchange = await self.channel.declare_exchange(
            name="file_upload_exchange_task30",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.file_upload_queue = await self.channel.declare_queue(
            name="file_upload_queue_task30",
            durable=True
        )
        await self.file_upload_queue.bind(
            exchange=self.file_upload_exchange,
            routing_key="file_upload_task30.key"
        )
        print("File Upload Queue connected...")
        #--------------------------
        # File Processing Exchange
        #--------------------------
        self.file_process_exchange = await self.channel.declare_exchange(
            name="file_process_exchange_task30",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.file_process_queue = await self.channel.declare_queue(
            name="file_process_queue_task30",
            durable=True
        )
        await self.file_process_queue.bind(
            exchange=self.file_process_exchange,
            routing_key="file_process_task30.key"
        )
        print("File Process Queue connected...")
        #--------------------------
        # File Embedding Exchange
        #--------------------------
        self.file_embedding_exchange = await self.channel.declare_exchange(
            name="file_embedding_exchange_task30",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.file_embedding_queue = await self.channel.declare_queue(
            name="file_embedding_queue_task30",
            durable=True
        )
        await self.file_embedding_queue.bind(
            exchange=self.file_embedding_exchange,
            routing_key="file_embedding_task30.key"
        )
        print("File Embedding Queue connected...")
        #------------------------
        #   Retry Exchange
        #------------------------
        self.retry_exchange = await self.channel.declare_exchange(
            name="retry_exchange_task30",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True
        )
        for i in range(self.attempts):
            queue = await self.channel.declare_queue(
                name=f"retry{i}_queue_task30",
                durable=True,
                arguments={
                    "x-message-ttl": 5000 * (2 ** i),
                }
            )
            await queue.bind(
                exchange=self.retry_exchange,
                routing_key=f"retry_{i}_task30.key",
            )
            print(f"Retry Queue created : routing_key: retry_{i}_task30.key.")
        #------------------------
        #   DLQ Exchange
        #------------------------
        self.dlq_exchange = await self.channel.declare_exchange(
            name="dlq_exchange_task30",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True,
        )
        self.dlq = await self.channel.declare_queue(
            name="dlq_task30",
            durable=True,
        )
        await self.dlq.bind(
            exchange=self.dlq_exchange,
            routing_key="dlq_task30.key",
        )
        print("DLQ is connected...")
        print("Rabbtimq Task30 is connected....")


    async def close(self) -> None:
        if self.connection:
            await self.connection.close()
            print("Rabbitmq Task 30 closed...")