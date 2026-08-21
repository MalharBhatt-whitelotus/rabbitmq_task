import json
import signal
import asyncio
from aio_pika import IncomingMessage

from rabbitmq_t29 import RabbitmqConnectionTask29


class RabbitmqConsumerTsk29:


    def __init__(self, url: str) -> None:
        self.rabbitmq = RabbitmqConnectionTask29(url)
        self.consumer_tag = None


    async def consumer(self, message: IncomingMessage) -> None:
        body = json.loads(message.body.decode())
        print("\n===================")
        await self.process(body)
        print("===================")
        await message.ack()

    async def process(self, body) -> None:
        print("Message is processing...")
        print(f"Message ID: {body.get("event")}")
        for i in range(1, 11):
            await asyncio.sleep(1)
            print(f"message processed: {i}/10")
        print("Message processed succesfully..")


    async def start_consumer(self) -> None:
        self.consumer_tag = await self.rabbitmq.main_queue.consume(self.consumer)


    async def stop_consumer(self) -> None:
        if self.consumer_tag and self.rabbitmq.main_queue:
            await self.rabbitmq.main_queue.cancel(self.consumer_tag)


    async def main(self) -> None:
        await self.rabbitmq.connect()
        await self.start_consumer()
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)
        try:
            await stop_event.wait()
        finally:
            print("Stopping consumer....")
            await self.stop_consumer()
            await self.rabbitmq.close()


if __name__ == "__main__":
    asyncio.run(
        RabbitmqConsumerTsk29("amqp://guest:guest@localhost:5672").main()
    )