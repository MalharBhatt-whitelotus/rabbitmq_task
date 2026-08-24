import aio_pika


class RabbitmqConnectionTask28:


    def __init__(self, url: str) -> None:
        self.url = url
        self.conneciton = None
        self.channel = None
        self.main_exchange = None
        self.main_queue = None


    async def connect(self) -> None:
        self.connection = await aio_pika.connect_robust(self.url)
        self.channel = await self.connection.channel(publisher_confirms=True)
        await self.channel.set_qos(prefetch_count=1)
        self.main_exchange = await self.channel.declare_exchange(
            name="main_exchange_task28",
            type=aio_pika.ExchangeType.DIRECT,
            durable=True
        )
        self.main_queue = await self.channel.declare_queue(
            name="main_queue_task28",
            durable=True
        )
        await self.main_queue.bind(
            exchange=self.main_exchange,
            routing_key="main_task28.key"
        )


    async def close(self) -> None:
        if self.connection:
            await self.connection.close()