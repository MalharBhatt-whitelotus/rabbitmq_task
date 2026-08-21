import json
import signal
import asyncio
from aio_pika import IncomingMessage

from rabbitmq_t27 import RabbitmqConnectionTask27

class RabbitmqDLQConsumerTask27:

    def __init__(self, url: str) -> None:
        self.rabbitmq = RabbitmqConnectionTask27(url=url)
        self.dlq_consumer_tag = None


    async def dlq_consumer(self, message: IncomingMessage):
        try:
            body = json.loads(message.body.decode())
            headers = message.headers
            print("\n========== DEAD LETTER ==========")
            print(f"Event: {body.get('event')}")
            print(f"File ID: {body.get('file_id')}")
            print(f"Retry Count: {headers.get('retry_count')}")
            print(f"Failure Reason: {body.get('failure_reason')}")
            print(f"Original Exchange: {body.get('original_exchange')}")
            print(f"Original Routing Key: {body.get('original_routing_key')}")
            print(f"Timestamp: {body.get('timestamp')}")
            print("=================================\n")
            await asyncio.sleep(3)
            await message.ack()
        except Exception as exc:
            print(f"Something went wrong...{exc}")
            raise


    async def start_dlq_consumer(self):
        self.dlq_consumer_tag = await self.rabbitmq.dlq.consume(self.dlq_consumer)


    async def stop_dlq_consumer(self):
        if self.dlq_consumer_tag and self.rabbitmq.dlq:
            await self.rabbitmq.dlq.cancel(self.dlq_consumer_tag)

        
    async def dlq_main(self) -> None:
        await self.rabbitmq.connect()
        await self.start_dlq_consumer()
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)
        try:
            await stop_event.wait()
        finally:
            print("Shuting down dlq consumer...")
            await self.stop_dlq_consumer()
            await self.rabbitmq.close()


if __name__ == "__main__":
    asyncio.run(
        RabbitmqDLQConsumerTask27("amqp://guest:guest@localhost:5672").dlq_main()
    )