import json
import signal
import asyncio
from aio_pika import IncomingMessage

from rabbitmq_t28 import RabbitmqConnectionTask28


class RabbitmqConsumerTask28:


    def __init__(self, url: str) -> None:
        self.rabbitmq = RabbitmqConnectionTask28(url)
        self.consumer_tag = None


    async def consumer(self, message: IncomingMessage) -> None:
            body = json.loads(message.body.decode())
            await asyncio.sleep(1)
            print("\n=====================")
            print(body)
            print("=====================")
            await message.ack()


    async def start_consumer(self):
        self.consumer_tag = await self.rabbitmq.main_queue.consume(self.consumer)


    async def stop_consumer(self):
        if self.consumer_tag and self.rabbitmq.main_queue:
            await self.rabbitmq.main_queue.cancel(self.consumer_tag)


    async def main(self):
        await self.rabbitmq.connect()
        await self.start_consumer()
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig, stop_event.set
            )
        try:
            await stop_event.wait()
        finally:
            print("Stopping the consumer..")
            await self.stop_consumer()
            await self.rabbitmq.close()


if __name__ == "__main__":
    asyncio.run(
        RabbitmqConsumerTask28("amqp://guest:guest@localhost:5672").main()
    )