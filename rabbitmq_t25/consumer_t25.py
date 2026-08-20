import asyncio
from random import randint
from aio_pika import IncomingMessage

from rabbitmq_t25 import RabbitmqConnectionTask25

class RabbitmqConsumerTask25:


    def __init__(self, url: str) -> None:
        self.rabbitmq = RabbitmqConnectionTask25(url)
        self.time = randint(1, 10)


    async def consume(self, message: IncomingMessage) -> None:
        await self.process()
        await message.ack()
        print("Message is acknowledged...")


    async def process(self):
        print(f"Message processing...for {self.time}")
        await asyncio.sleep(self.time)
        print("Message is processed...")


    async def main(self):
        await self.rabbitmq.connect()
        await self.rabbitmq.main_queue.consume(self.consume)
        await asyncio.Future()